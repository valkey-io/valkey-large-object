//! RESP protocol client with byte-level TCP accounting.
//!
//! Tracks exactly how many bytes are sent/received over TCP so we can prove
//! that object data travels over EFA, not TCP.

use eyre::{eyre, Result};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::time::Instant;

/// A RESP connection that counts every byte crossing the TCP socket.
pub struct RespConnection {
    stream: TcpStream,
    pub tcp_bytes_sent: u64,
    pub tcp_bytes_received: u64,
}

impl RespConnection {
    pub fn connect(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true)?;
        Ok(Self {
            stream,
            tcp_bytes_sent: 0,
            tcp_bytes_received: 0,
        })
    }

    /// Send a RESP command and read the reply. Returns (reply_string, elapsed_ms).
    /// Prints the command as "> CMD arg1 arg2 ..." and reply as "< reply (Xms)".
    pub fn command(&mut self, args: &[&str]) -> Result<(String, f64)> {
        // Encode RESP array
        let mut cmd = format!("*{}\r\n", args.len());
        for arg in args {
            cmd.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
        }

        // Print command (truncate long args like EFA addresses for readability)
        let display_args: Vec<String> = args
            .iter()
            .map(|a| {
                if a.len() > 20 {
                    format!("{}...({} bytes)", &a[..16], a.len())
                } else {
                    a.to_string()
                }
            })
            .collect();
        println!("  > {}", display_args.join(" "));

        // Send
        let cmd_bytes = cmd.as_bytes();
        self.stream.write_all(cmd_bytes)?;
        self.stream.flush()?;
        self.tcp_bytes_sent += cmd_bytes.len() as u64;

        // Receive + time
        let t0 = Instant::now();
        let (reply, reply_bytes) = self.read_reply()?;
        let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;
        self.tcp_bytes_received += reply_bytes as u64;

        // Print reply (truncate long bulk strings)
        let display_reply = reply.lines().next().unwrap_or("").to_string();
        let display = if display_reply.len() > 80 {
            format!("{}...", &display_reply[..77])
        } else {
            display_reply
        };
        println!("  < {} ({:.2} ms)", display, elapsed_ms);

        Ok((reply, elapsed_ms))
    }

    /// Send a command without printing (for warmup/cleanup).
    pub fn command_silent(&mut self, args: &[&str]) -> Result<(String, f64)> {
        let mut cmd = format!("*{}\r\n", args.len());
        for arg in args {
            cmd.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
        }

        let cmd_bytes = cmd.as_bytes();
        self.stream.write_all(cmd_bytes)?;
        self.stream.flush()?;
        self.tcp_bytes_sent += cmd_bytes.len() as u64;

        let t0 = Instant::now();
        let (reply, reply_bytes) = self.read_reply()?;
        let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;
        self.tcp_bytes_received += reply_bytes as u64;

        Ok((reply, elapsed_ms))
    }

    /// Read one complete RESP reply. Returns (parsed_string, raw_byte_count).
    fn read_reply(&mut self) -> Result<(String, usize)> {
        let mut reader = BufReader::new(self.stream.try_clone()?);
        let mut total_bytes = 0;

        let mut first_line = String::new();
        let n = reader.read_line(&mut first_line)?;
        total_bytes += n;

        match first_line.chars().next() {
            Some('+') | Some('-') => {
                // Simple string or error
                Ok((first_line, total_bytes))
            }
            Some(':') => {
                // Integer
                Ok((first_line, total_bytes))
            }
            Some('$') => {
                // Bulk string
                let len: i64 = first_line[1..].trim().parse()?;
                if len < 0 {
                    Ok(("$-1\r\n".to_string(), total_bytes))
                } else {
                    let mut data = vec![0u8; (len + 2) as usize]; // +2 for \r\n
                    reader.read_exact(&mut data)?;
                    total_bytes += data.len();
                    let mut result = first_line;
                    result.push_str(&String::from_utf8_lossy(&data));
                    Ok((result, total_bytes))
                }
            }
            Some('*') => {
                // Array
                let count: i64 = first_line[1..].trim().parse()?;
                let mut result = first_line.clone();
                for _ in 0..count {
                    let mut element_line = String::new();
                    let n = reader.read_line(&mut element_line)?;
                    total_bytes += n;
                    result.push_str(&element_line);
                    if element_line.starts_with('$') {
                        let elen: i64 = element_line[1..].trim().parse()?;
                        if elen >= 0 {
                            let mut data = vec![0u8; (elen + 2) as usize];
                            reader.read_exact(&mut data)?;
                            total_bytes += data.len();
                            result.push_str(&String::from_utf8_lossy(&data));
                        }
                    }
                }
                Ok((result, total_bytes))
            }
            _ => Err(eyre!("Unknown RESP type: {:?}", first_line)),
        }
    }

    /// Parse the first bulk string from a RESP array reply.
    pub fn parse_array_first_bulk(reply: &str) -> Result<String> {
        let lines: Vec<&str> = reply.lines().collect();
        for (i, line) in lines.iter().enumerate() {
            if line.starts_with('$') {
                let len: usize = line[1..].trim().parse()?;
                if i + 1 < lines.len() {
                    let data = &lines[i + 1][..len.min(lines[i + 1].len())];
                    return Ok(data.to_string());
                }
            }
        }
        Err(eyre!("No bulk string found in reply: {}", reply))
    }
}

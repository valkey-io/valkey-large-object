//! Eviction: a SET or COPY whose budget is full destroys other objects instead of failing. The
//! budget is the DRAM arena in Dram mode and `nvme-maxmemory` in Tiered mode. Neither needs a key
//! name: a victim's id is tombstoned on the reclaim list (`storage::reclaim`), where its key reads as
//! a miss until the scaling cron deletes it. Whether to evict is the server's call
//! (`eviction_allowed`: `maxmemory` set and a policy other than `noeviction`); the policy does not
//! pick the victims.
//!
//! It runs on the event-loop thread without awaiting, so the arena bytes a victim held are free
//! before the SET retries. A Tiered victim's file is unlinked off the event loop
//! (`storage::Evicted`), which can stall on the filesystem; until then its bytes stay in the ledger,
//! and the SET reserves against them as pending (`storage::nvme::nvme_shortfall`).
//!
//! Layout: counters and shared helpers, then Dram eviction, Tiered eviction, and the directory
//! sampler Tiered eviction draws its victims with.

use std::collections::HashSet;
use std::ffi::c_long;
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

use rand::RngExt;
use valkey_module::Context;

use crate::data_type::ObjectId;
use crate::storage::context::{ObjectContext, SegmentBuffer};
use crate::storage::reclaim::RECLAIM_LIST;
use crate::storage::{lock_inflight, DRAMPool, Evicted, InflightGuard};

// ─── Counters and shared helpers ────────────────────────────────────────────────────────────

// Counters exposed via `INFO largeobj`.

/// Objects evicted from the arena since module load.
pub static EVICTIONS_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Arena bytes the victims held.
pub static RECLAIMED_BYTES_TOTAL: AtomicU64 = AtomicU64::new(0);
/// SETs that still failed after eviction ran: objects destroyed for nothing.
pub static EVICTION_FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Candidates passed over because something still held them, counted per look: a pinned `Arc` in
/// Dram mode, a file with a request in flight (`InflightGuard`) in Tiered mode.
pub static PINNED_SKIPS_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Tiered counterparts. Reclaimed is each victim's whole `disk_len`.
pub static DISK_EVICTIONS_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static DISK_RECLAIMED_BYTES_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static DISK_EVICTION_FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Victim files nobody will remove: their key was freed while eviction owned the unlink, and the
/// unlink failed. They stay on disk and charged until shutdown.
pub static DISK_LEAKED_FILES_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Allocations refused after enough victims were freed: bytes free but not usable together.
pub static SATISFY_REFUSALS_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Evictions abandoned after `DRAM_MAX_ROUNDS` refusals: more victims will not fix the
/// fragmentation.
pub static FRAGMENTATION_ABORTS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// The O(1) refusals every budget makes before searching: `need` above the whole budget can never
/// be served, and nothing resident leaves nothing to evict.
fn cannot_be_satisfied(need: u64, ceiling: u64, resident: u64) -> bool {
    need > ceiling || resident == 0
}

// ─── Dram ───────────────────────────────────────────────────────────────────────────────────

// Dram eviction: freeing arena bytes by destroying objects from the one segment the new object
// will land in, and only once they cover the shortfall (`DRAMPool::evict_in`, which lists their
// ids). The bytes they free can still be unusable together (the allocator needs room for each
// chunk), so a refused allocation takes another victim, up to `MAX_ROUNDS`.

/// Most victims one request may take. Bounds the time the event loop spends in one SET.
const DRAM_MAX_VICTIMS: usize = 4096;

/// Allocations tried, each after freeing more, before the fragmentation is blamed.
const DRAM_MAX_ROUNDS: usize = 3;

/// Allocate `need` bytes in the arena, destroying resident objects to make room.
pub fn alloc_by_evicting(ctx: &Context, need: usize) -> Option<Vec<SegmentBuffer>> {
    debug_assert_eq!(
        crate::operating_mode(),
        crate::OperatingMode::Dram,
        "evicting for the arena destroys objects, which only Dram mode may do"
    );
    if !crate::eviction_allowed(ctx) {
        return None;
    }
    let pool = crate::storage::get_dram_pool();

    // No object spans segments, so nothing larger than one fits. Nothing resident means nothing to
    // evict, which also covers an arena of in-flight SET buffers no key points at yet.
    let segment_size = crate::dram_segment_size() as u64;
    if cannot_be_satisfied(need as u64, segment_size, pool.object_count() as u64) {
        return None;
    }

    let mut evicted = false;
    let buffers = make_room(pool, need, &mut evicted);
    // Only runs that evicted something: they paid and got nothing.
    if buffers.is_none() && evicted {
        EVICTION_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
    buffers
}

fn make_room(pool: &DRAMPool, need: usize, evicted: &mut bool) -> Option<Vec<SegmentBuffer>> {
    for _ in 0..DRAM_MAX_ROUNDS {
        let (segment, short) = pool.alloc_target(need)?;
        // Short of nothing: the bytes fit but the allocator could not place them, so free one.
        let (victims, pinned) = pool.evict_in(segment, short.max(1), DRAM_MAX_VICTIMS);
        PINNED_SKIPS_TOTAL.fetch_add(pinned, Ordering::Relaxed);
        let victims = victims?;

        tally(&victims);
        *evicted = true;
        // Each victim's `Arc` is its last: dropping returns its buffers to the arena.
        drop(victims);

        if let Some(buffers) = pool.alloc_exact_in(segment, need) {
            return Some(buffers);
        }
        SATISFY_REFUSALS_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
    FRAGMENTATION_ABORTS_TOTAL.fetch_add(1, Ordering::Relaxed);
    None
}

fn tally(victims: &[(ObjectId, Arc<ObjectContext>)]) {
    // The aligned size the arena handed out, not the user length: that is what freeing gives back.
    let bytes: u64 = victims
        .iter()
        .flat_map(|(_, obj)| &obj.buffers)
        .map(|buf| u64::from(buf.len))
        .sum();
    EVICTIONS_TOTAL.fetch_add(victims.len() as u64, Ordering::Relaxed);
    RECLAIMED_BYTES_TOTAL.fetch_add(bytes, Ordering::Relaxed);
}

// ─── Tiered ─────────────────────────────────────────────────────────────────────────────────

// Tiered eviction: victims are random files of the object directory, listed on the reclaim list
// by id and counted as pending, which lets the SET that asked reserve against them. The unlink,
// and the credit for it, are left to that SET's write task (`Evicted`), off the event loop.
// Their keys read as misses until the scaling cron deletes them. Random whatever the policy (a directory has no
// access stats), and nothing is destroyed unless the victims cover the shortfall, unless the
// budget is over its cap already.

/// Most victims one request may take. Each costs a stat and some bookkeeping on the event loop,
/// so this keeps a SET to a fraction of a millisecond.
const DISK_MAX_VICTIMS: usize = 64;

/// Directory draws per round of selection.
const MAX_DRAWS: usize = 16;

/// Consecutive draws that name nothing new before the directory is taken to be small.
const STALE_DRAWS: usize = 8;

/// Entries listed from a small directory, which random draws are not sure to have met whole.
const SCAN_LIMIT: usize = 2048;

/// Rounds of selecting and evicting before a request gives up on a shortfall that races keep
/// reopening.
const DISK_MAX_ROUNDS: usize = 3;

/// Make `need` bytes of `nvme-maxmemory` available, never evicting `keep` (a COPY's source).
/// `None` means the budget cannot serve it. The victims are listed and pending: their files are
/// still to unlink, which credits the ledger. Main thread only, before the SET's task is spawned.
pub fn make_disk_room(ctx: &Context, need: u64, keep: Option<ObjectId>) -> Option<Evicted> {
    let budget = crate::nvme_maxmemory();
    if budget == 0 {
        unreachable!("nvme-maxmemory is unlimited, yet a reservation was refused");
    }
    if cannot_be_satisfied(need, budget, crate::storage::nvme::nvme_disk_usage())
        || !crate::eviction_allowed(ctx)
    {
        return None;
    }

    let dir = crate::nvme_dir();
    let mut sampler = match DirSampler::open(&dir) {
        Ok(sampler) => sampler,
        Err(e) => {
            valkey_module::logging::log_warning(format!(
                "largeobj: cannot read the object directory {dir} to evict: {e}"
            ));
            return None;
        }
    };

    let mut evicted = Evicted::default();
    let covered = evict_until_covered(&mut sampler, need, keep, &mut evicted);
    // Only runs that evicted something: they paid and got nothing. Their files still go.
    if !covered && !evicted.is_empty() {
        DISK_EVICTION_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
    covered.then_some(evicted)
}

/// The headroom `need` asks for, given what the ledger holds now and the victims already claimed
/// (concurrent frees only shrink it).
fn shortfall(need: u64) -> u64 {
    crate::storage::nvme::nvme_shortfall(need)
}

fn evict_until_covered(
    sampler: &mut DirSampler,
    need: u64,
    keep: Option<ObjectId>,
    evicted: &mut Evicted,
) -> bool {
    let mut budget = DISK_MAX_VICTIMS;
    for _ in 0..DISK_MAX_ROUNDS {
        let missing = shortfall(need);
        if missing == 0 {
            return true;
        }
        let picked = select(sampler, missing, keep, budget);
        // Lowering `nvme-maxmemory` under the usage leaves an overage no one request may cover:
        // shed what was found even if it is short, or refusals that destroy nothing never end it.
        let over_cap = shortfall(0) > 0;
        if !picked.covers() && !over_cap {
            return false;
        }
        budget -= picked.victims.len();
        let dram_pool = crate::storage::get_dram_pool();
        for (id, _) in picked.victims {
            if let Some((size, pin)) = take(sampler, id) {
                // The promoted copy would otherwise hold arena bytes until the key is deleted.
                dram_pool.remove_cached_copy(&id);
                crate::storage::nvme::add_pending_free(size);
                evicted.push(id, size, pin);
                DISK_RECLAIMED_BYTES_TOTAL.fetch_add(size, Ordering::Relaxed);
                DISK_EVICTIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    shortfall(need) == 0
}

/// Victims for one round, with their on-disk sizes, in the order they will be taken.
struct Selection {
    need: u64,
    max_victims: usize,
    /// Ids already looked at, and the COPY source that is never a victim.
    seen: HashSet<ObjectId>,
    victims: Vec<(ObjectId, u64)>,
    freed: u64,
}

impl Selection {
    fn covers(&self) -> bool {
        self.freed >= self.need
    }

    fn full(&self) -> bool {
        self.covers() || self.victims.len() >= self.max_victims
    }

    /// Consider a batch in order, until the request is covered. Returns how many of its ids no
    /// earlier batch had named.
    fn offer(&mut self, sampler: &DirSampler, ids: &mut Vec<ObjectId>) -> usize {
        ids.retain(|id| self.seen.insert(*id));
        let fresh = ids.len();
        // A file a request is using, or a victim claimed earlier and not yet unlinked, is pinned.
        {
            let pins = lock_inflight();
            let before = ids.len();
            ids.retain(|id| !pins.contains_key(id));
            PINNED_SKIPS_TOTAL.fetch_add((before - ids.len()) as u64, Ordering::Relaxed);
        }
        for &id in ids.iter() {
            if self.full() {
                break;
            }
            // Unlinked since it was listed, or empty: nothing to free either way.
            if let Some(size) = sampler.size_of(id).ok().filter(|&size| size > 0) {
                self.victims.push((id, size));
                self.freed += size;
            }
        }
        fresh
    }
}

/// Pick random objects whose sizes cover `need`, at most `max_victims`. Destroys nothing.
fn select(
    sampler: &mut DirSampler,
    need: u64,
    keep: Option<ObjectId>,
    max_victims: usize,
) -> Selection {
    let mut picked = Selection {
        need,
        max_victims,
        seen: keep.into_iter().collect(),
        victims: Vec::new(),
        freed: 0,
    };
    let (mut ids, mut stale) = (Vec::new(), 0);
    for _ in 0..MAX_DRAWS {
        if picked.full() || stale >= STALE_DRAWS {
            break;
        }
        ids.clear();
        sampler.draw(&mut ids);
        stale = if picked.offer(sampler, &mut ids) == 0 {
            stale + 1
        } else {
            0
        };
    }
    // Draws that keep naming nothing new have met a small directory whole: list it to be sure.
    if !picked.full() && stale >= STALE_DRAWS {
        ids.clear();
        sampler.scan(&mut ids, SCAN_LIMIT);
        picked.offer(sampler, &mut ids);
    }
    picked
}

/// List `id` as reclaimed and pin it until its unlink, unless it is listed or pinned already.
/// Under the list lock, so the teardown of a key freed after this finds the entry and leaves the
/// unlink to us.
fn claim(id: ObjectId) -> Option<InflightGuard> {
    let mut pin = None;
    RECLAIM_LIST.add_unless(id, || {
        pin = InflightGuard::new_if_unpinned(id);
        pin.is_none()
    });
    pin
}

/// List `id`; the caller counts its bytes as pending and unlinks the file. Returns the size and the
/// victim pin, or `None` if `id` was pinned, was listed already, or its file is gone.
fn take(sampler: &DirSampler, id: ObjectId) -> Option<(u64, InflightGuard)> {
    let pin = claim(id)?;
    // Nothing may serve the object from here on, whatever its key still says: close the cached fd.
    if let Some(pool) = crate::storage::FD_POOL.get() {
        pool.remove(id);
    }
    // The file's own teardown may have finished since the draw: counting a gone file as pending
    // would admit a write the disk has no room for.
    if let Ok(size) = sampler.size_of(id) {
        return Some((size, pin));
    }
    RECLAIM_LIST.remove(&id);
    None
}

// ─── Directory sampler ──────────────────────────────────────────────────────────────────────

// Random victims straight from the object directory: every object is a file
// `{nvme_dir}/{oid:016x}.dat`, so an entry names its object and nothing else is kept.
//
// A draw is one `getdents64` at a random cookie. The filesystem decides what a cookie is (ext4
// hashes names, others use a byte offset) and `lseek(SEEK_END)` reports the top of that range, so
// a uniform cookie below it is a uniform place in the directory. Where it reports no range
// (tmpfs), or the cookies read nothing, the sampler reads on from where the last request stopped:
// the next entries rather than random ones, which is still a valid choice.

/// Bytes handed to one `getdents64`: about 25 object files, at 40 bytes a record.
const BUF_LEN: usize = 1024;

/// Random cookies tried before reading on sequentially. A cookie past the last entry narrows the
/// range for the next try, so a reported range wider than the real one (XFS counts its cookies in
/// 8-byte units but reports the directory's size in bytes) still converges.
const RANDOM_TRIES: usize = 8;

/// Entries a draw returns. A fixed-length window that wraps past the end of the directory puts
/// every entry in the same number of windows; cut short, the last entries would be favoured.
const WINDOW: usize = 24;

/// Where sequential reads resume. Shared by requests so they walk the whole directory.
static SEQ_CURSOR: AtomicI64 = AtomicI64::new(0);

/// Draws that fell back to sequential reads since module load.
pub static SEQUENTIAL_DRAWS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// The object a directory entry names, if it names one.
fn parse_file_name(name: &[u8]) -> Option<ObjectId> {
    let hex = name.strip_suffix(b".dat")?;
    if hex.len() != 16 {
        return None;
    }
    let mut id = 0u64;
    for &digit in hex {
        let value = match digit {
            b'0'..=b'9' => digit - b'0',
            b'a'..=b'f' => digit - b'a' + 10,
            _ => return None,
        };
        id = id << 4 | u64::from(value);
    }
    Some(ObjectId(id))
}

pub struct DirSampler {
    dir: File,
    path: String,
    /// The top of the filesystem's cookie space, if it says.
    span: Option<u64>,
    buf: [u8; BUF_LEN],
}

/// What one `getdents64` read came back with.
struct Batch {
    /// Bytes the kernel returned: zero is the end of the directory.
    bytes: usize,
    /// Entries that name objects, appended to the caller's list.
    objects: usize,
    /// The cookie after the last entry read.
    next: i64,
}

impl DirSampler {
    pub fn open(path: &str) -> io::Result<Self> {
        let dir = File::options()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC)
            .open(path)?;
        // SAFETY: `dir` is a live directory fd; `lseek` only moves its offset.
        let end = unsafe { libc::lseek(dir.as_raw_fd(), 0, libc::SEEK_END) };
        Ok(Self {
            dir,
            path: path.to_owned(),
            span: u64::try_from(end).ok().filter(|&end| end > 0),
            buf: [0; BUF_LEN],
        })
    }

    /// The size of `id`'s file, which is the ledger's charge for it.
    pub fn size_of(&self, id: ObjectId) -> io::Result<u64> {
        std::fs::metadata(id.file_path(&self.path)).map(|meta| meta.len())
    }

    /// Append the objects at one random place in the directory to `out`, in random order:
    /// `WINDOW` of them, or the next ones in directory order where there is no random access.
    /// Nothing is appended if the directory has nothing readable.
    pub fn draw(&mut self, out: &mut Vec<ObjectId>) {
        let first = out.len();
        if let Some(mut bound) = self.span {
            for _ in 0..RANDOM_TRIES {
                let cookie = rand::rng().random_range(0..bound);
                if self.window_at(cookie as i64, out) > 0 {
                    break;
                }
                bound = cookie.max(1);
            }
        }
        if out.len() == first {
            SEQUENTIAL_DRAWS_TOTAL.fetch_add(1, Ordering::Relaxed);
            self.read_on(out);
        }
        shuffle(&mut out[first..]);
    }

    /// Append every object in the directory to `out`, at most `limit` of them (give or take a
    /// batch), from the start. For a directory too small for random draws to be sure of finding
    /// what is there.
    pub fn scan(&mut self, out: &mut Vec<ObjectId>, limit: usize) {
        let start = out.len();
        let mut cookie = 0;
        while out.len() - start < limit {
            let batch = self.read_at(cookie, out);
            if batch.bytes == 0 {
                break;
            }
            cookie = batch.next;
        }
    }

    /// `WINDOW` entries from `cookie` on, wrapping once past the end of the directory. Zero if
    /// `cookie` is past the last entry, for the caller to draw again.
    fn window_at(&mut self, cookie: i64, out: &mut Vec<ObjectId>) -> usize {
        let start = out.len();
        let first = self.read_at(cookie, out);
        if first.bytes == 0 {
            return 0;
        }
        let (mut at, mut wrapped) = (first.next, false);
        while out.len() - start < WINDOW {
            let batch = self.read_at(at, out);
            if batch.bytes > 0 {
                at = batch.next;
            } else if wrapped {
                break;
            } else {
                (at, wrapped) = (0, true);
            }
        }
        out.truncate(start + WINDOW);
        out.len() - start
    }

    /// Read on from where the last sequential read stopped, wrapping once at the end.
    fn read_on(&mut self, out: &mut Vec<ObjectId>) {
        let mut cookie = SEQ_CURSOR.load(Ordering::Relaxed);
        for _ in 0..2 {
            // A batch holding no object (only `.`, `..` or foreign names) still moves the cursor.
            loop {
                let batch = self.read_at(cookie, out);
                if batch.bytes == 0 {
                    cookie = 0;
                    break;
                }
                cookie = batch.next;
                if batch.objects > 0 {
                    SEQ_CURSOR.store(cookie, Ordering::Relaxed);
                    return;
                }
            }
        }
        SEQ_CURSOR.store(0, Ordering::Relaxed);
    }

    /// One `getdents64` at `cookie`.
    fn read_at(&mut self, cookie: i64, out: &mut Vec<ObjectId>) -> Batch {
        let fd = self.dir.as_raw_fd();
        // SAFETY: `fd` is a live directory fd, and `buf` is valid for `BUF_LEN` writable bytes.
        let bytes = unsafe {
            if libc::lseek(fd, cookie, libc::SEEK_SET) < 0 {
                return Batch {
                    bytes: 0,
                    objects: 0,
                    next: 0,
                };
            }
            libc::syscall(
                libc::SYS_getdents64,
                fd as c_long,
                self.buf.as_mut_ptr(),
                BUF_LEN as c_long,
            )
        };
        let bytes = usize::try_from(bytes).unwrap_or(0);
        let mut batch = Batch {
            bytes,
            objects: 0,
            next: cookie,
        };
        // `struct linux_dirent64`: u64 ino, i64 off, u16 reclen, u8 type, then the NUL-terminated name.
        let mut at = 0;
        while at + 19 <= bytes {
            let reclen = usize::from(u16::from_ne_bytes([self.buf[at + 16], self.buf[at + 17]]));
            if reclen < 19 || at + reclen > bytes {
                break;
            }
            let mut off = [0u8; 8];
            off.copy_from_slice(&self.buf[at + 8..at + 16]);
            batch.next = i64::from_ne_bytes(off);
            let name = &self.buf[at + 19..at + reclen];
            let name = &name[..name.iter().position(|&b| b == 0).unwrap_or(name.len())];
            if let Some(id) = parse_file_name(name) {
                out.push(id);
                batch.objects += 1;
            }
            at += reclen;
        }
        batch
    }
}

/// Fisher-Yates: a batch is consecutive in directory order, so taking its first entries would
/// favour whichever follows the largest gap in the hash.
fn shuffle(ids: &mut [ObjectId]) {
    let mut rng = rand::rng();
    for i in (1..ids.len()).rev() {
        ids.swap(i, rng.random_range(0..=i));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    fn scratch_dir(name: &str) -> String {
        let dir = std::env::temp_dir().join(format!("tiered-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir.to_str().unwrap().to_owned()
    }

    #[test]
    fn a_pinned_or_listed_file_cannot_be_claimed() {
        let id = ObjectId(0x7a1e_0001);
        let request = InflightGuard::new(id);
        assert!(claim(id).is_none(), "a request is using it");
        assert!(!RECLAIM_LIST.contains(&id));
        drop(request);

        let victim = claim(id).expect("unpinned");
        assert!(RECLAIM_LIST.contains(&id));
        assert!(lock_inflight().contains_key(&id), "a victim stays pinned");
        assert!(claim(id).is_none(), "already listed");
        drop(victim);
        RECLAIM_LIST.remove(&id);
    }

    #[test]
    fn take_lists_a_file_and_skips_a_stale_one() {
        let dir = scratch_dir("take");
        let sampler = DirSampler::open(&dir).unwrap();
        let id = ObjectId(0x7a1e_0002);
        std::fs::write(id.file_path(&dir), b"x").unwrap();

        let victim = take(&sampler, id);
        assert_eq!(victim.as_ref().map(|(size, _)| *size), Some(1));
        assert!(
            RECLAIM_LIST.contains(&id),
            "its key reads as a miss from here on"
        );

        // The file is gone, so listing it again would be stale: no tombstone is left.
        RECLAIM_LIST.remove(&id);
        drop(victim);
        std::fs::remove_file(id.file_path(&dir)).unwrap();
        assert!(take(&sampler, id).is_none());
        assert!(!RECLAIM_LIST.contains(&id));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_selection_covers_the_need_without_pinned_listed_or_kept_ids() {
        let dir = scratch_dir("select");
        for id in 0x7a1e_1001..=0x7a1e_100a {
            std::fs::write(ObjectId(id).file_path(&dir), [0u8; 100]).unwrap();
        }
        let (writing, keep, listed) = (
            ObjectId(0x7a1e_1003),
            ObjectId(0x7a1e_1005),
            ObjectId(0x7a1e_1007),
        );
        let _request = InflightGuard::new(writing);
        let _victim = claim(listed).expect("unpinned");
        let mut sampler = DirSampler::open(&dir).unwrap();

        let picked = select(&mut sampler, 350, Some(keep), DISK_MAX_VICTIMS);
        assert!(picked.covers() && picked.freed < 500);
        assert!(picked
            .victims
            .iter()
            .all(|&(id, _)| id != writing && id != keep && id != listed));

        let all = select(&mut sampler, 10_000, Some(keep), DISK_MAX_VICTIMS);
        assert!(!all.covers());
        assert_eq!(
            all.victims.len(),
            7,
            "everything but the writing, the kept and the listed"
        );
        RECLAIM_LIST.remove(&listed);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A scratch directory of object files `(id, length)`, removed on drop.
    struct TestDir(PathBuf);

    impl TestDir {
        fn with(files: impl IntoIterator<Item = (u64, usize)>) -> Self {
            Self::in_dir(&std::env::temp_dir(), files).unwrap()
        }

        fn in_dir(base: &Path, files: impl IntoIterator<Item = (u64, usize)>) -> io::Result<Self> {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let dir = base.join(format!(
                "nvme-sampler-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&dir)?;
            let dir = Self(dir);
            for (id, len) in files {
                let path = ObjectId(id).file_path(dir.0.to_str().unwrap());
                std::fs::write(path, vec![0u8; len])?;
            }
            Ok(dir)
        }

        fn sampler(&self) -> DirSampler {
            DirSampler::open(self.0.to_str().unwrap()).unwrap()
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn names_round_trip_and_foreign_names_are_not_objects() {
        for id in [0, 1, 0xdead_beef, u64::MAX] {
            let path = ObjectId(id).file_path(".");
            assert_eq!(parse_file_name(&path.as_bytes()[2..]), Some(ObjectId(id)));
        }
        for foreign in [
            "",
            "..",
            "x.dat",
            "0000000000000001.tmp",
            "000000000000001.dat",
            "00000000000000001.dat",
            "000000000000000G.dat",
            "000000000000000A.dat",
            "+000000000000001.dat",
        ] {
            assert_eq!(parse_file_name(foreign.as_bytes()), None, "{foreign}");
        }
    }

    #[test]
    fn a_draw_is_a_shuffled_window_of_objects_that_knows_their_sizes() {
        let dir = TestDir::with((1..=300).map(|id| (id, 100 + id as usize)));
        std::fs::write(dir.0.join("stray.txt"), b"x").unwrap();
        let mut sampler = dir.sampler();
        let random = sampler.span.is_some();

        let mut firsts = HashSet::new();
        for _ in 0..200 {
            let mut ids = Vec::new();
            sampler.draw(&mut ids);
            // Every window has the same length, the wrap at the end of the directory included.
            assert!(if random {
                ids.len() == WINDOW
            } else {
                !ids.is_empty()
            });
            for id in &ids {
                assert_eq!(sampler.size_of(*id).unwrap(), 100 + id.0);
            }
            firsts.insert(ids[0]);
        }
        assert!(firsts.len() > 50, "{} distinct first picks", firsts.len());
        assert!(sampler.size_of(ObjectId(999)).is_err());
    }

    /// tmpfs reports no cookie range, and counts its cookies from 0, which stands in for a
    /// filesystem that reports more than it counts (XFS: bytes against 8-byte units).
    #[test]
    fn a_span_wider_than_the_cookie_range_still_draws_randomly() {
        let Ok(dir) = TestDir::in_dir(Path::new("/dev/shm"), (1..=300).map(|id| (id, 0))) else {
            return;
        };
        let mut sampler = dir.sampler();
        if sampler.span.is_some() {
            return;
        }
        sampler.span = Some(8 * 302);

        let (mut seen, mut windows) = (HashSet::new(), 0);
        for _ in 0..1000 {
            let mut ids = Vec::new();
            sampler.draw(&mut ids);
            windows += usize::from(ids.len() == WINDOW);
            seen.extend(ids);
        }
        assert!(windows > 950, "{windows} random windows of 1000");
        assert_eq!(seen.len(), 300);
    }

    #[test]
    fn without_random_access_draws_walk_the_directory_and_wrap() {
        let dir = TestDir::with((1..=100).map(|id| (id, 0)));
        let mut sampler = dir.sampler();
        sampler.span = None;
        SEQ_CURSOR.store(0, Ordering::Relaxed);

        let mut seen = HashSet::new();
        for _ in 0..40 {
            let mut ids = Vec::new();
            sampler.draw(&mut ids);
            assert!(!ids.is_empty());
            seen.extend(ids);
        }
        assert_eq!(seen.len(), 100, "successive draws cover the directory");
    }

    #[test]
    fn a_scan_lists_a_small_directory_exactly_and_an_empty_one_yields_nothing() {
        let small = TestDir::with((1..=7).map(|id| (id, 0)));
        let mut all = Vec::new();
        small.sampler().scan(&mut all, 1000);
        all.sort();
        assert_eq!(all, (1..=7).map(ObjectId).collect::<Vec<_>>());

        let big = TestDir::with((1..=200).map(|id| (id, 0)));
        let mut limited = Vec::new();
        big.sampler().scan(&mut limited, 50);
        assert!((50..80).contains(&limited.len()), "{}", limited.len());

        let empty = TestDir::with([]);
        let mut none = Vec::new();
        let mut sampler = empty.sampler();
        sampler.draw(&mut none);
        sampler.scan(&mut none, 10);
        assert!(none.is_empty());
    }
}

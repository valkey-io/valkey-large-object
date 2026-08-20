use std::env;
use std::path::PathBuf;

fn main() {
    // Try to find libfabric via pkg-config. If it fails (e.g. dev desktop without
    // libfabric installed), we still let the build succeed — the transport layer
    // will gracefully report EFA as unavailable at runtime.
    let lib = match pkg_config::Config::new()
        .atleast_version("1.0")
        .probe("libfabric")
    {
        Ok(lib) => lib,
        Err(_) => {
            // Check the EFA installer path as fallback.
            // Set PKG_CONFIG_PATH for the pkg-config crate's internal process.
            let efa_pkgconfig = "/opt/amazon/efa/lib64/pkgconfig";
            if std::path::Path::new(efa_pkgconfig).exists() {
                // Prepend to existing PKG_CONFIG_PATH
                let existing = env::var("PKG_CONFIG_PATH").unwrap_or_default();
                let new_path = if existing.is_empty() {
                    efa_pkgconfig.to_string()
                } else {
                    format!("{}:{}", efa_pkgconfig, existing)
                };
                // SAFETY: build scripts are single-threaded.
                unsafe { env::set_var("PKG_CONFIG_PATH", &new_path) };
                match pkg_config::Config::new()
                    .atleast_version("1.0")
                    .probe("libfabric")
                {
                    Ok(lib) => lib,
                    Err(_) => {
                        println!(
                            "cargo:warning=libfabric not found — EFA transport will be unavailable"
                        );
                        println!("cargo:rustc-cfg=no_efa");
                        return;
                    }
                }
            } else {
                println!("cargo:warning=libfabric not found — EFA transport will be unavailable");
                println!("cargo:rustc-cfg=no_efa");
                return;
            }
        }
    };

    // Link libfabric
    println!("cargo:rustc-link-lib=fabric");

    // Add EFA lib path if it exists (for runtime linking)
    if PathBuf::from("/opt/amazon/efa/lib64").exists() {
        println!("cargo:rustc-link-search=native=/opt/amazon/efa/lib64");
    }

    // Generate bindings
    let mut builder = bindgen::Builder::default()
        .header("src/transport/wrapper.h")
        .allowlist_function("fi_.*")
        .allowlist_type("fi_.*")
        .allowlist_type("fid_.*")
        .allowlist_var("FI_.*")
        .allowlist_type("fi_info")
        .allowlist_type("iovec")
        .derive_default(true)
        .derive_debug(true);

    // Add include paths from pkg-config
    for path in &lib.include_paths {
        builder = builder.clang_arg(format!("-I{}", path.display()));
    }

    // Also check EFA installer include path
    let efa_include = PathBuf::from("/opt/amazon/efa/include");
    if efa_include.exists() {
        builder = builder.clang_arg(format!("-I{}", efa_include.display()));
    }

    let bindings = builder
        .generate()
        .expect("Unable to generate libfabric bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("libfabric_bindings.rs"))
        .expect("Couldn't write bindings");
}

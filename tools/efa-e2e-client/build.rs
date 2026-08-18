use std::env;
use std::path::PathBuf;

fn main() {
    // Find libfabric via pkg-config
    let lib = pkg_config::Config::new()
        .atleast_version("1.0")
        .probe("libfabric")
        .expect("libfabric not found via pkg-config. Install libfabric-devel or set PKG_CONFIG_PATH=/opt/amazon/efa/lib64/pkgconfig");

    // Link libfabric
    println!("cargo:rustc-link-lib=fabric");

    // Generate bindings
    let mut builder = bindgen::Builder::default()
        .header("wrapper.h")
        .allowlist_function("fi_.*")
        .allowlist_type("fi_.*")
        .allowlist_type("fid_.*")
        .allowlist_var("FI_.*")
        .allowlist_type("fi_info")
        .derive_default(true)
        .derive_debug(true);

    // Add include paths from pkg-config
    for path in &lib.include_paths {
        builder = builder.clang_arg(format!("-I{}", path.display()));
    }

    let bindings = builder
        .generate()
        .expect("Unable to generate libfabric bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings");
}

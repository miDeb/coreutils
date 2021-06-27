#![no_main]

use std::io::Write;
use std::process::Command;
use std::process::Stdio;

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: (&str, &str)| {
    fn get_out(cmd: &str, a: &str, b: &str) -> Vec<u8> {
        let mut uu = Command::new(cmd)
            .arg("-V")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut stdin = uu.stdin.take().unwrap();
        stdin.write_all(a.as_bytes()).unwrap();
        stdin.write_all("\n".as_bytes()).unwrap();
        stdin.write_all(b.as_bytes()).unwrap();
        stdin.write_all("\n".as_bytes()).unwrap();
        drop(stdin);
        uu.wait_with_output().unwrap().stdout
    }

    if data.0.contains('\0') || data.1.contains('\0') {
        return;
    }

    assert_eq!(
        get_out("target/release/sort", data.0, data.1),
        get_out("sort", data.0, data.1)
    )
}); //   "S\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0",
    // "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0",

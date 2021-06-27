#[macro_use]
extern crate afl;

use std::io::Write;
use std::process::Command;
use std::process::Stdio;

fn main() {
    fuzz!(|data: &[u8]| {
        if let Ok(data) = std::str::from_utf8(data) {
            fn get_out(cmd: &str, data: &str) -> Vec<u8> {
                let mut uu = Command::new(cmd)
                    .arg("-V")
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .spawn()
                    .unwrap();
                let mut stdin = uu.stdin.take().unwrap();
                stdin.write_all(data.as_bytes()).unwrap();
                drop(stdin);
                uu.wait_with_output().unwrap().stdout
            }

            if data.contains("..")
                || data.contains(".\n")
                || data.ends_with(".")
                || data.contains("\0")
            {
                return;
            }

            assert_eq!(
                get_out("/home/michael/devprojects/uutils/target/release/sort", data),
                get_out("sort", data)
            )
        }
    });
}

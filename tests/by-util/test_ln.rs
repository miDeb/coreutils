use crate::common::util::*;
use std::path::PathBuf;

#[test]
fn test_relative_src_already_symlink() {
    let (at, mut ucmd) = at_and_ucmd!();
    at.mkdir("wrapper");
    at.touch("wrapper/file1");
    at.symlink_file("wrapper/file1", "wrapper/file2");
    let result = ucmd.arg("-sr").arg("wrapper/file2").arg("wrapper/file3").succeeds();
    dbg!(result.stdout());
    dbg!(result.stderr());
    assert_eq!(at.resolve_link("wrapper/file3"), "wrapper/file1");
}

#[test]
fn test_parent_is_symlink() {
    let (at, mut ucmd) = at_and_ucmd!();
    at.mkdir("dir1");
    at.symlink_dir("dir1", "dir_to_dir1");
    at.mkdir("dir1/nested_dir1");
    at.touch("dir1/nested_dir1/file1");
    let result = ucmd
        .arg("-rs")
        //.curdir_join("dir_to_dir1/nested_dir1")
        .arg("dir_to_dir1/nested_dir1/file1")
        .arg("dir_to_dir1/nested_dir1/file_to_file1")
        .succeeds();
    print!("{}", result.stdout_str());
    print!("{}", result.stderr_str());
    dbg!(at.resolve_link("dir_to_dir1/nested_dir1/file_to_file1"));
}

use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::Path;

use tempdir::TempDir;

use crate::{file_to_lines_iter, FileMerger, GlobalSettings, OwningLine};

/// Iterator that provides sorted `T`s
pub struct ExtSortedIterator<'a> {
    file_merger: FileMerger<'a>,
    // Keep tmp_dir around, it is deleted when dropped.
    _tmp_dir: TempDir,
}

impl<'a> Iterator for ExtSortedIterator<'a> {
    type Item = OwningLine;
    fn next(&mut self) -> Option<Self::Item> {
        self.file_merger.next()
    }
}

/// Sort (based on `compare`) the `T`s provided by `unsorted` and return an
/// iterator
///
/// # Panics
///
/// This method can panic due to issues writing intermediate sorted chunks
/// to disk.
pub fn ext_sort(
    unsorted: impl Iterator<Item = OwningLine>,
    settings: &GlobalSettings,
) -> ExtSortedIterator {
    let tmp_dir = crash_if_err!(1, TempDir::new_in(&settings.tmp_dir, "uutils_sort"));

    let mut total_read = 0;
    let mut chunk = Vec::new();

    let mut chunks_read = 0;
    let mut file_merger = FileMerger::new(settings);

    // make the initial chunks on disk
    for seq in unsorted {
        let seq_size = seq.estimate_size();
        total_read += seq_size;

        chunk.push(seq);

        if total_read >= settings.buffer_size && chunk.len() >= 2 {
            super::sort_by(&mut chunk, &settings);

            let file_path = tmp_dir.path().join(chunks_read.to_string());
            write_chunk(settings, &file_path, &mut chunk);
            chunk.clear();
            total_read = 0;
            chunks_read += 1;

            file_merger.push_file(Box::new(file_to_lines_iter(file_path, settings).unwrap()))
        }
    }
    // write the last chunk
    if !chunk.is_empty() {
        super::sort_by(&mut chunk, &settings);

        let file_path = tmp_dir.path().join(chunks_read.to_string());
        write_chunk(
            settings,
            &tmp_dir.path().join(chunks_read.to_string()),
            &mut chunk,
        );

        file_merger.push_file(Box::new(file_to_lines_iter(file_path, settings).unwrap()));
    }
    ExtSortedIterator {
        file_merger,
        _tmp_dir: tmp_dir,
    }
}

fn write_chunk(settings: &GlobalSettings, file: &Path, chunk: &mut Vec<OwningLine>) {
    let new_file = crash_if_err!(1, OpenOptions::new().create(true).append(true).open(file));
    let mut buf_write = BufWriter::new(new_file);
    for s in chunk {
        crash_if_err!(1, buf_write.write_all(s.borrow_line().as_bytes()));
        crash_if_err!(
            1,
            buf_write.write_all(if settings.zero_terminated { "\0" } else { "\n" }.as_bytes(),)
        );
    }
    crash_if_err!(1, buf_write.flush());
}

pub mod rewrite {
    use std::{
        fs::OpenOptions,
        io::{BufWriter, ErrorKind, Read, Write},
        path::Path,
    };

    use memchr::memchr_iter;
    use tempdir::TempDir;

    use crate::{
        external_sort::ExtSortedIterator, file_to_lines_iter, sort_by, FileMerger, GlobalSettings,
        Line, OwningLine,
    };

    pub fn ext_sort<'a>(
        files: &mut impl Iterator<Item = Box<dyn Read>>,
        settings: &'a GlobalSettings,
    ) -> impl Iterator<Item = OwningLine> + 'a {
        let tmp_dir = crash_if_err!(1, TempDir::new_in(&settings.tmp_dir, "uutils_sort"));

        let mut chunks_read = 0usize;

        let separator = if settings.zero_terminated {
            b'\0'
        } else {
            b'\n'
        };
        // document
        let mut chunk_offset = 0;
        let mut chunk: Vec<u8> = vec![0; settings.buffer_size / 7];
        let mut file = files.next().unwrap();

        let mut lines = Vec::<Line<'_, &str>>::new();
        loop {
            eprintln!("reading...");
            let (read, should_continue) =
                read_to_chunk(&mut file, files, &mut chunk, chunk_offset, separator);

            do_sort(
                read,
                &mut lines,
                separator,
                settings,
                &tmp_dir.path().join(chunks_read.to_string()),
            );
            chunks_read += 1;

            if !should_continue {
                break;
            }
            let read_len = read.len();
            chunk_offset = chunk.len() - read_len;
            println!("draining...");
            chunk.drain(..read_len);
            println!("resizing...");
            chunk.resize(chunk.len() + read_len, 0);
        }

        drop(lines);
        drop(chunk);

        let mut file_merger = FileMerger::new(settings);

        for i in 0..chunks_read {
            file_merger.push_file(Box::new(
                file_to_lines_iter(tmp_dir.path().join(i.to_string()), settings).unwrap(),
            ));
        }
        ExtSortedIterator {
            file_merger,
            _tmp_dir: tmp_dir,
        }
    }

    fn do_sort<'a>(
        mut read: &'a str,
        lines: &mut Vec<Line<'static, &'static str>>,
        separator: u8,
        settings: &GlobalSettings,
        output_file: &Path,
    ) {
        if read.ends_with(separator as char) {
            read = &read[..read.len() - 1];
        }

        assert!(lines.is_empty());
        let lines = unsafe {
            // SAFETY: Casting the vector covariantly is safe because the vector is empty.
            std::mem::transmute::<&mut Vec<Line<'static, &'static str>>, &mut Vec<Line<'a, &'a str>>>(
                lines,
            )
        };
        eprintln!("extending...");
        lines.extend(
            read.split(separator as char)
                .map(|line| Line::create(line, settings)),
        );
        eprintln!("sorting...");

        sort_by(lines, settings);
        eprintln!("writing...");

        write_chunk(output_file, lines, separator);
        eprintln!("clearing...");
        lines.clear();
    }

    fn read_to_chunk<'chunk>(
        file: &mut Box<dyn Read>,
        next_files: &mut impl Iterator<Item = Box<dyn Read>>,
        chunk: &'chunk mut Vec<u8>,
        start_offset: usize,
        separator: u8,
    ) -> (&'chunk str, bool) {
        let mut buff = &mut chunk[start_offset..];
        loop {
            match file.read(buff) {
                Ok(0) => {
                    if buff.is_empty() {
                        // chunk is full
                        let mut sep_iter = memchr_iter(separator, &chunk).rev();
                        let last_line_end = sep_iter.next();
                        if sep_iter.next().is_some() {
                            // We read enough lines.
                            let end = last_line_end.unwrap();
                            return (crash_if_err!(1, std::str::from_utf8(&chunk[..=end])), true);
                        } else {
                            // We need to read more lines
                            let len = chunk.len();
                            // resize the vector to 10 KB more
                            chunk.resize(len + 1024 * 10, 0);
                            buff = &mut chunk[len..];
                        }
                    } else {
                        // This file is empty.
                        if let Some(next_file) = next_files.next() {
                            // There is another file.
                            *file = next_file;
                        } else {
                            // This was the last file.
                            let len = buff.len();
                            let read = &chunk[..chunk.len() - len];
                            return (crash_if_err!(1, std::str::from_utf8(read)), false);
                        }
                    }
                }
                Ok(n) => {
                    buff = &mut buff[n..];
                }
                Err(e) if e.kind() == ErrorKind::Interrupted => {
                    // retry
                }
                Err(e) => {
                    crash!(1, "{}", e)
                }
            }
        }
    }

    fn write_chunk(file: &Path, chunk: &mut Vec<Line<'_, &str>>, separator: u8) {
        let new_file = crash_if_err!(1, OpenOptions::new().create(true).append(true).open(file));
        let mut buf_write = BufWriter::with_capacity(1_000_000, new_file);
        for s in chunk {
            crash_if_err!(1, buf_write.write_all(s.borrow_line().as_bytes()));
            crash_if_err!(1, buf_write.write_all(&[separator]));
        }
        crash_if_err!(1, buf_write.flush());
    }
}

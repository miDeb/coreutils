//  * This file is part of the uutils coreutils package.
//  *
//  * (c) Michael Debertol <michael.debertol..AT..gmail.com>
//  *
//  * For the full copyright and license information, please view the LICENSE
//  * file that was distributed with this source code.

use std::io::{BufWriter, Write};
use std::path::Path;
use std::{
    fs::OpenOptions,
    io::{ErrorKind, Read},
    sync::mpsc::{Receiver, SyncSender},
    thread,
};

use memchr::memchr_iter;
use ouroboros::self_referencing;
use tempdir::TempDir;

use crate::{
    file_to_lines_iter, sort_by, BorrowedLine, FileMerger, GlobalSettings, Line, OwningLine,
};

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

fn write_chunk(file: &Path, chunk: &[BorrowedLine<'_>], separator: u8) {
    let new_file = crash_if_err!(1, OpenOptions::new().create(true).write(true).open(file));
    let mut buf_write = BufWriter::new(new_file);
    for s in chunk {
        crash_if_err!(1, buf_write.write_all(s.borrow_line().as_bytes()));
        crash_if_err!(1, buf_write.write_all(&[separator]));
    }
    crash_if_err!(1, buf_write.flush());
}

fn read_to_chunk<'chunk>(
    file: &mut Box<dyn Read>,
    next_files: &mut impl Iterator<Item = Box<dyn Read>>,
    chunk: &'chunk mut Vec<u8>,
    start_offset: usize,
    separator: u8,
) -> (usize, bool) {
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
                        return (end + 1, true);
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
                        return (chunk.len() - len, false);
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

#[self_referencing]
struct Payload {
    string: Vec<u8>,
    #[borrows(string)]
    #[not_covariant]
    lines: Vec<BorrowedLine<'this>>,
}

fn reader_writer(
    mut files: impl Iterator<Item = Box<dyn Read>>,
    tmp_dir: &TempDir,
    separator: u8,
    buffer_size: usize,
    settings: GlobalSettings,
    receiver: Receiver<Payload>,
    sender: SyncSender<Payload>,
) -> usize {
    let mut sender_option = Some(sender);

    let mut file = files.next().unwrap();

    let mut carry_over = vec![];
    // kick things off with two reads
    for _ in 0..2 {
        if let Some(sender) = sender_option.as_ref() {
            let mut buffer = vec![0u8; buffer_size];
            if buffer.len() < carry_over.len() {
                buffer.resize(carry_over.len() + 10 * 1024, 0);
            }
            buffer[..carry_over.len()].copy_from_slice(&carry_over);
            let (read, should_continue) = read_to_chunk(
                &mut file,
                &mut files,
                &mut buffer,
                carry_over.len(),
                separator,
            );
            carry_over.clear();
            carry_over.extend_from_slice(&buffer[read..]);
            let first_payload = Payload::new(buffer, |buf| {
                let mut first_lines = Vec::new();
                let read = crash_if_err!(1, std::str::from_utf8(&buf[..read]));
                load_lines(read, &mut first_lines, separator, &settings);
                first_lines
            });
            sender.send(first_payload).unwrap();
            if !should_continue {
                sender_option = None;
            }
        }
    }

    let mut chunks_read = 0;
    loop {
        let mut payload = match receiver.recv() {
            Ok(it) => it,
            _ => return chunks_read,
        };
        let lines: Vec<BorrowedLine<'static>> = payload.with_lines_mut(|lines| {
            write_chunk(
                &tmp_dir.path().join(chunks_read.to_string()),
                lines,
                separator,
            );
            chunks_read += 1;
            let mut replacement = vec![];
            std::mem::swap(lines, &mut replacement);
            replacement.clear();
            unsafe {
                // Safe to transmute to a vector of lines with longer lifetime, vec is empty
                std::mem::transmute::<Vec<BorrowedLine<'_>>, Vec<BorrowedLine<'static>>>(replacement)
            }
        });

        let mut buffer = payload.into_heads().string;

        if let Some(sender) = sender_option.as_ref() {
            if buffer.len() < carry_over.len() {
                buffer.resize(carry_over.len() + 10 * 1024, 0);
            }
            buffer[..carry_over.len()].copy_from_slice(&carry_over);
            let (read, should_continue) = read_to_chunk(
                &mut file,
                &mut files,
                &mut buffer,
                carry_over.len(),
                separator,
            );
            carry_over.clear();
            carry_over.extend_from_slice(&buffer[read..]);

            let payload = Payload::new(buffer, |buf| {
                let mut lines = unsafe {
                    // Safe to transmute to a vector of lines with shorter lifetime, vec is empty
                    std::mem::transmute(lines)
                };
                let read = crash_if_err!(1, std::str::from_utf8(&buf[..read]));
                load_lines(read, &mut lines, separator, &settings);
                lines
            });
            sender.send(payload).unwrap();
            if !should_continue {
                sender_option = None;
            }
        }
    }
}

fn load_lines<'a>(
    mut read: &'a str,
    lines: &mut Vec<BorrowedLine<'a>>,
    separator: u8,
    settings: &GlobalSettings,
) {
    if read.ends_with(separator as char) {
        read = &read[..read.len() - 1];
    }

    lines.extend(
        read.split(separator as char)
            .map(|line| Line::create(line, settings)),
    );
}

fn sorter(receiver: Receiver<Payload>, sender: SyncSender<Payload>, settings: GlobalSettings) {
    while let Ok(mut payload) = receiver.recv() {
        eprintln!("received lines, sorting...");
        payload.with_lines_mut(|lines| sort_by(lines, &settings));
        eprintln!("sorted, sending...");
        sender.send(payload).unwrap();
        eprintln!("sent, waiting for next chunk...");
    }
    eprintln!("not waiting anymore, that was the last one");
}

pub fn ext_sort<'a>(
    files: &mut impl Iterator<Item = Box<dyn Read>>,
    settings: &'a GlobalSettings,
) -> impl Iterator<Item = OwningLine> + 'a {
    let tmp_dir = crash_if_err!(1, TempDir::new_in(&settings.tmp_dir, "uutils_sort"));
    let (sorted_sender, sorted_receiver) = std::sync::mpsc::sync_channel(1);
    let (recycled_sender, recycled_receiver) = std::sync::mpsc::sync_channel(1);
    thread::spawn({
        let settings = settings.clone();
        move || sorter(recycled_receiver, sorted_sender, settings)
    });
    let chunks_read = reader_writer(
        files,
        &tmp_dir,
        if settings.zero_terminated {
            b'\0'
        } else {
            b'\n'
        },
        settings.buffer_size / 10,
        settings.clone(),
        sorted_receiver,
        recycled_sender,
    );
    let mut file_merger = FileMerger::new(settings);
    for i in 0..chunks_read {
        file_merger.push_file(Box::new(
            file_to_lines_iter(tmp_dir.path().join(i.to_string()), settings).unwrap(),
        ));
    }
    eprintln!("created file merger");
    ExtSortedIterator {
        file_merger,
        _tmp_dir: tmp_dir,
    }
}

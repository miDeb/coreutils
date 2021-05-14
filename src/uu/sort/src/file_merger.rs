//! Merge files, fast.
//!
//! We achieve performance by splitting the tasks of sorting and writing, and reading and parsing between two threads.
//! The threads communicate over channels. There's one channel per file in the direction reader -> sorter, but only
//! one channel from the sorter back to the reader. The channels to the sorter are used to send the read chunks.
//! It reads the next chunk from it whenever it needs the next one when it runs out of lines from the previous read
//! for the file. The channel back from the sorter to the reader on the other hand has two purposes: To allow the reader
//! to reuse memory allocations and to tell the reader which file to read from next.
//! TODO: this would easily allow for multiple readers. Do we need more than one, are readers the bottleneck? We could
//! dynamically launch more readers. Problem: carry_over
//!

use std::{
    cmp::Ordering,
    io::Read,
    iter,
    marker::PhantomData,
    sync::mpsc::{Receiver, Sender, SyncSender},
};

use binary_heap_plus::{BinaryHeap, FnComparator};
use ouroboros::self_referencing;

use crate::{
    ext_sort::{load_lines, read_to_chunk},
    BorrowedLine, GlobalSettings,
};

fn merge(files: Vec<String>) {}
#[self_referencing]
struct Payload {
    string: Vec<u8>,
    #[borrows(string)]
    #[not_covariant]
    lines: Vec<BorrowedLine<'this>>,
    // The amount of bytes we need to carry over.
    offset: usize,
}
fn reader(
    rec: Receiver<(usize, Payload)>,
    files: Vec<Box<dyn Read>>,
    senders: Vec<SyncSender<Payload>>,
) {
    while let Ok((file_idx, recycle)) = rec.recv() {}
}

struct MergeableFile {
    current_chunk: Payload,
    // index into current_chunk. let's not make this struct self-referential as well.
    line_idx: usize,
    chunk_receiver: Receiver<Payload>,
}

struct FileMerger</*'a,*/ T>
where
    T: Fn(&MergeableFile, &MergeableFile) -> Ordering, /*+ 'a*/
{
    heap: BinaryHeap<MergeableFile, T>,
    request_sender: Sender<(usize, Payload)>,
    //_p: PhantomData<&'a ()>,
}

impl<T> Iterator for FileMerger<T>
where
    T: Fn(&MergeableFile, &MergeableFile) -> Ordering,
{
    type Item;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

fn sorter(
    settings: &'_ GlobalSettings,
    mut files: Vec<Box<dyn Read>>,
    receivers: Vec<Receiver<Payload>>,
    sender: Sender<(usize, Payload)>,
) -> FileMerger<impl for<'r, 's> Fn(&'r MergeableFile, &'s MergeableFile) -> Ordering + '_> {
    let separator = if settings.zero_terminated {
        b'\0'
    } else {
        b'\n'
    };
    let files_len = files.len();
    let mut mergeable_files = Vec::with_capacity(files_len);
    let mut next_payloads = Vec::with_capacity(files_len);
    for file in &mut files {
        // This first read is not parallelized...
        let mut buffer = vec![0u8; settings.buffer_size / files_len / 50];
        let (read, _) = read_to_chunk(file, &mut iter::empty(), &mut buffer, 0, separator);
        let carry_amount = buffer.len() - read;
        let mut next_buffer = vec![0u8; settings.buffer_size / files_len / 50];
        if next_buffer.len() < carry_amount {
            // Edge case: The buffer is smaller than the carry-over
            next_buffer.resize(carry_amount + 10 * 1024, 0);
        }
        next_buffer[..carry_amount].copy_from_slice(&buffer[read..]);
        next_payloads.push(Payload::new(next_buffer, |_| Vec::new(), carry_amount));
        let payload = Payload::new(
            buffer,
            |buf| {
                let mut lines = Vec::new();
                let read = crash_if_err!(1, std::str::from_utf8(&buf[..read]));
                load_lines(read, &mut lines, separator, &settings);
                lines
            },
            0,
        );
        mergeable_files.push(payload);
    }
    for (idx, payload) in next_payloads.into_iter().enumerate() {
        sender.send((idx, payload)).unwrap();
    }
    FileMerger {
        heap: BinaryHeap::from_vec_cmp(
            mergeable_files
                .into_iter()
                .zip(receivers.into_iter())
                .map(|(payload, receiver)| MergeableFile {
                    current_chunk: payload,
                    line_idx: 0,
                    chunk_receiver: receiver,
                })
                .collect(),
            make_cmp(settings),
        ),
        request_sender: sender, //_p: PhantomData,
    }
}

fn cmp(a: &MergeableFile, b: &MergeableFile) -> Ordering {
    panic!()
}

fn make_cmp(
    settings: &'_ GlobalSettings,
) -> impl for<'r, 's> Fn(&'r MergeableFile, &'s MergeableFile) -> Ordering + '_ {
    move |a, b| {
        settings;
        Ordering::Equal
    }
}

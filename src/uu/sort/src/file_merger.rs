//! Merge files, fast.
//!
//! We achieve performance by splitting the tasks of sorting and writing, and reading and parsing between two threads.
//! The threads communicate over channels. There's one channel per file in the direction reader -> sorter, but only
//! one channel from the sorter back to the reader. The channels to the sorter are used to send the read chunks.
//! It reads the next chunk from it whenever it needs the next one when it runs out of lines from the previous read
//! for the file. The channel back from the sorter to the reader on the other hand has two purposes: To allow the reader
//! to reuse memory allocations and to tell the reader which file to read from next.
//! TODO: this would easily allow for multiple readers. Do we need more than one, are readers the bottleneck? We could
//! dynamically launch more readers.
//!
//! The implementation here has one big problem: It relies on OwnedLines, i.e. copies each line into a newly allocated String.
//! That seems to be very bad for the system memory allocator, but using MiMalloc seems to mitigate that. Nonetheless, a non-allocating
//! version would probably be much better.

use std::{
    cmp::Ordering,
    collections::VecDeque,
    ffi::OsStr,
    io::{BufRead, BufReader, Read, Split},
    sync::mpsc::{channel, sync_channel, Receiver, Sender, SyncSender},
    thread,
};

use binary_heap_plus::BinaryHeap;
use compare::Compare;

use crate::{compare_by, open, GlobalSettings, Line, OwningLine};

pub fn merge<'a>(files: &[impl AsRef<OsStr>], settings: &'a GlobalSettings) -> FileMerger<'a> {
    let (recycled_sender, recycled_receiver) = channel();
    let mut loaded_senders = Vec::with_capacity(files.len());
    let mut loaded_receivers = Vec::with_capacity(files.len());
    let mut buffered_files = Vec::with_capacity(files.len());
    for file in files {
        if let Some(file) = open(file) {
            let (sender, receiver) = sync_channel(2);
            loaded_receivers.push(receiver);
            loaded_senders.push(sender);
            buffered_files.push(BufReader::new(file).split(if settings.zero_terminated {
                b'\0'
            } else {
                b'\n'
            }));
        }
    }

    thread::spawn({
        let settings = settings.clone();
        || reader(recycled_receiver, buffered_files, loaded_senders, settings)
    });
    sorter(settings, loaded_receivers, recycled_sender)
}

fn reader(
    rec: Receiver<(usize, VecDeque<OwningLine>)>,
    mut files: Vec<Split<BufReader<Box<dyn Read + Send>>>>,
    senders: Vec<SyncSender<VecDeque<OwningLine>>>,
    settings: GlobalSettings,
) {
    while let Ok((file_idx, mut recycle)) = rec.recv() {
        assert!(recycle.is_empty());
        const MAX_READ: usize = 8 * 1024;
        let mut read = 0;
        for line in (&mut files[file_idx]).flatten() {
            read += line.len();

            let line = Line::create(String::from_utf8(line).unwrap().into_boxed_str(), &settings);
            recycle.push_back(line);
            if read >= MAX_READ {
                break;
            }
        }
        /*println!(
            "read {}:\n {:?}",
            file_idx,
            recycle
                .iter()
                .map(|line| line.borrow_line().as_ref())
                .collect::<Vec<&str>>()
        );*/
        senders[file_idx].send(recycle).unwrap();
    }
}

pub struct MergeableFile {
    current_chunk: VecDeque<OwningLine>,
    receiver: Receiver<VecDeque<OwningLine>>,
    file_number: usize,
}

pub struct FileMerger<'a> {
    heap: BinaryHeap<MergeableFile, FileComparator<'a>>,
    request_sender: Sender<(usize, VecDeque<OwningLine>)>,
    //_p: PhantomData<&'a ()>,
}

impl<'a> Iterator for FileMerger<'a> {
    type Item = OwningLine;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(mut file) = self.heap.peek_mut() {
            if file.current_chunk.len() > 1 {
                return Some(file.current_chunk.pop_front().unwrap());
            } else {
                let mut next_chunk = file.receiver.recv().unwrap();
                if !next_chunk.is_empty() {
                    let ret = file.current_chunk.pop_front().unwrap();
                    std::mem::swap(&mut file.current_chunk, &mut next_chunk);
                    self.request_sender
                        .send((file.file_number, next_chunk))
                        .unwrap();
                    return Some(ret);
                }
            }
        }
        self.heap.pop().map(|mut f| {
            assert_eq!(f.current_chunk.len(), 1);
            f.current_chunk.pop_front().unwrap()
        })
    }
}

fn sorter(
    settings: &'_ GlobalSettings,
    receivers: Vec<Receiver<VecDeque<OwningLine>>>,
    sender: Sender<(usize, VecDeque<OwningLine>)>,
) -> FileMerger<'_> {
    for _ in 0..2 {
        for index in 0..receivers.len() {
            sender.send((index, VecDeque::new())).unwrap();
        }
    }

    let mergeable_files: Vec<MergeableFile> = receivers
        .into_iter()
        .enumerate()
        .filter_map(|(file_number, receiver)| {
            let first_chunk = receiver.recv().unwrap();
            if first_chunk.is_empty() {
                None
            } else {
                Some(MergeableFile {
                    current_chunk: first_chunk,
                    receiver,
                    file_number,
                })
            }
        })
        .collect();

    FileMerger {
        heap: BinaryHeap::from_vec_cmp(mergeable_files, FileComparator { settings }),
        request_sender: sender, //_p: PhantomData,
    }
}

struct FileComparator<'a> {
    settings: &'a GlobalSettings,
}

impl<'a> Compare<MergeableFile> for FileComparator<'a> {
    fn compare(&self, a: &MergeableFile, b: &MergeableFile) -> Ordering {
        let mut cmp = compare_by(
            &a.current_chunk.front().unwrap(),
            &b.current_chunk.front().unwrap(),
            self.settings,
        );
        if cmp == Ordering::Equal {
            cmp = a.file_number.cmp(&b.file_number);
        }
        cmp.reverse()
    }
}

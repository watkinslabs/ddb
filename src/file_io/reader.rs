// Streaming line reader for low memory usage
use crate::error::Result;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

pub struct LineReader {
    reader: BufReader<File>,
    line_number: usize,
}

impl LineReader {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);

        Ok(Self {
            reader,
            line_number: 0,
        })
    }

    pub fn current_line(&self) -> usize {
        self.line_number
    }
}

impl Iterator for LineReader {
    type Item = Result<String>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut line = String::new();

        match self.reader.read_line(&mut line) {
            Ok(0) => None, // EOF
            Ok(_) => {
                self.line_number += 1;
                Some(Ok(line))
            }
            Err(e) => Some(Err(e.into())),
        }
    }
}

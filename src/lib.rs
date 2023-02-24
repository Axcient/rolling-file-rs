//! A rolling file appender with customizable rolling conditions.
//! Includes built-in support for rolling conditions on date/time
//! (daily, hourly, every minute) and/or size.
//!
//! Follows a Debian-style naming convention for logfiles,
//! using basename, basename.1, ..., basename.N where N is
//! the maximum number of allowed historical logfiles.
//!
//! This is useful to combine with the tracing crate and
//! tracing_appender::non_blocking::NonBlocking -- use it
//! as an alternative to tracing_appender::rolling::RollingFileAppender.
//!
//! # Examples
//!
//! ```rust
//! # fn docs() {
//! # use rolling_file::*;
//! let file_appender = BasicRollingFileAppender::new(
//!     "/var/log/myprogram",
//!     RollingConditionBasic::new().daily().max_files(9),
//! ).unwrap();
//! # }
//! ```
#![deny(warnings)]

use chrono::prelude::*;
use std::{
    convert::TryFrom,
    ffi::OsString,
    fs,
    fs::{File, OpenOptions},
    io,
    io::{BufWriter, Write},
    path::Path,
};

/// Determines when a file should be "rolled over".
pub trait RollingCondition {
    /// Determine and return whether or not the file shoud be rolled over.
    fn should_rollover(&mut self, now: &DateTime<Local>, current_filesize: u64) -> bool;

    /// Determines the maximum number of files.
    fn max_files(&self) -> usize;
}

/// Determines how often a file should be rolled over
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum RollingFrequency {
    EveryDay,
    EveryHour,
    EveryMinute,
}

impl RollingFrequency {
    /// Calculates a datetime that will be different if data should be in
    /// different files.
    pub fn equivalent_datetime(&self, dt: &DateTime<Local>) -> DateTime<Local> {
        match self {
            RollingFrequency::EveryDay => Local
                .with_ymd_and_hms(dt.year(), dt.month(), dt.day(), 0, 0, 0)
                .unwrap(),
            RollingFrequency::EveryHour => Local
                .with_ymd_and_hms(dt.year(), dt.month(), dt.day(), dt.hour(), 0, 0)
                .unwrap(),
            RollingFrequency::EveryMinute => Local
                .with_ymd_and_hms(dt.year(), dt.month(), dt.day(), dt.hour(), dt.minute(), 0)
                .unwrap(),
        }
    }
}

/// Implements a rolling condition to never rolling.
pub struct NoRollingCondition {}

impl RollingCondition for NoRollingCondition {
    /// Always return false because it do not have to roll.
    fn should_rollover(&mut self, _now: &DateTime<Local>, _current_filesize: u64) -> bool {
        false
    }

    fn max_files(&self) -> usize {
        1
    }
}

impl Default for NoRollingCondition {
    /// Constructs a new NoRollingCondition.
    fn default() -> Self {
        Self {}
    }
}

/// Implements a rolling condition based on a certain frequency
/// and/or a size limit. The default condition is to rotate daily.
///
/// # Examples
///
/// ```rust
/// use rolling_file::*;
/// let c = RollingConditionBasic::new().daily();
/// let c = RollingConditionBasic::new().hourly().max_size(1024 * 1024);
/// ```
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct RollingConditionBasic {
    last_write_opt: Option<DateTime<Local>>,
    frequency_opt: Option<RollingFrequency>,
    max_size_opt: Option<u64>,
    max_files_opt: Option<usize>,
}

impl RollingConditionBasic {
    /// Constructs a new struct that does not yet have any condition set.
    pub fn new() -> Self {
        Self {
            last_write_opt: None,
            frequency_opt: None,
            max_size_opt: None,
            max_files_opt: None,
        }
    }

    /// Sets a condition to rollover on the given frequency
    pub fn frequency(mut self, x: RollingFrequency) -> Self {
        self.frequency_opt = Some(x);
        self
    }

    /// Sets a condition to rollover when the date changes
    pub fn daily(mut self) -> Self {
        self.frequency_opt = Some(RollingFrequency::EveryDay);
        self
    }

    /// Sets a condition to rollover when the date or hour changes
    pub fn hourly(mut self) -> Self {
        self.frequency_opt = Some(RollingFrequency::EveryHour);
        self
    }

    /// Sets a condition to rollover when a certain size is reached
    pub fn max_size(mut self, x: u64) -> Self {
        self.max_size_opt = Some(x);
        self
    }

    /// Sets a condition to rollover when a certain number of files is reached
    pub fn max_files(mut self, max_files: usize) -> Self {
        self.max_files_opt = Some(max_files);
        self
    }
}

impl Default for RollingConditionBasic {
    fn default() -> Self {
        RollingConditionBasic::new().frequency(RollingFrequency::EveryDay)
    }
}

impl RollingCondition for RollingConditionBasic {
    fn should_rollover(&mut self, now: &DateTime<Local>, current_filesize: u64) -> bool {
        let mut rollover = false;
        if let Some(frequency) = self.frequency_opt.as_ref() {
            if let Some(last_write) = self.last_write_opt.as_ref() {
                if frequency.equivalent_datetime(now) != frequency.equivalent_datetime(last_write) {
                    rollover = true;
                }
            }
        }
        if let Some(max_size) = self.max_size_opt.as_ref() {
            if current_filesize >= *max_size {
                rollover = true;
            }
        }
        self.last_write_opt = Some(*now);
        rollover
    }

    fn max_files(&self) -> usize {
        self.max_files_opt.unwrap_or(1)
    }
}

/// Writes data to a file, and "rolls over" to preserve older data in
/// a separate set of files. Old files have a Debian-style naming scheme
/// where we have base_filename, base_filename.1, ..., base_filename.N
/// where N is the maximum number of rollover files to keep.
#[derive(Debug)]
pub struct RollingFileAppender<RC>
where
    RC: RollingCondition,
{
    condition: RC,
    base_filename: OsString,
    buffer_capacity: Option<usize>,
    current_filesize: u64,
    writer_opt: Option<BufWriter<File>>,
}

impl<RC> RollingFileAppender<RC>
where
    RC: RollingCondition,
{
    /// Creates a new rolling file appender with the given condition.
    /// The parent directory of the base path must already exist.
    pub fn new<P>(path: P, condition: RC) -> io::Result<RollingFileAppender<RC>>
    where
        P: AsRef<Path>,
    {
        Self::_new(path, condition, None)
    }

    /// Creates a new rolling file appender with the given condition and write buffer capacity.
    /// The parent directory of the base path must already exist.
    pub fn new_with_buffer_capacity<P>(
        path: P,
        condition: RC,
        buffer_capacity: usize,
    ) -> io::Result<RollingFileAppender<RC>>
    where
        P: AsRef<Path>,
    {
        Self::_new(path, condition, Some(buffer_capacity))
    }

    fn _new<P>(path: P, condition: RC, buffer_capacity: Option<usize>) -> io::Result<RollingFileAppender<RC>>
    where
        P: AsRef<Path>,
    {
        let mut rfa = RollingFileAppender {
            condition,
            base_filename: path.as_ref().as_os_str().to_os_string(),
            buffer_capacity,
            current_filesize: 0,
            writer_opt: None,
        };
        // Fail if we can't open the file initially...
        rfa.open_writer_if_needed()?;
        Ok(rfa)
    }

    /// Determines the final filename, where n==0 indicates the current file
    fn filename_for(&self, n: usize) -> OsString {
        let mut f = self.base_filename.clone();
        if n > 0 {
            f.push(OsString::from(format!(".{}", n)))
        }
        f
    }

    /// Rotates old files to make room for a new one.
    /// This may result in the deletion of the oldest file
    fn rotate_files(&mut self) -> io::Result<()> {
        // ignore any failure removing the oldest file (may not exist)
        let _ = fs::remove_file(self.filename_for(self.condition.max_files().max(1)));
        let mut r = Ok(());
        for i in (0..self.condition.max_files().max(1)).rev() {
            let rotate_from = self.filename_for(i);
            let rotate_to = self.filename_for(i + 1);
            if let Err(e) = fs::rename(rotate_from, rotate_to).or_else(|e| match e.kind() {
                io::ErrorKind::NotFound => Ok(()),
                _ => Err(e),
            }) {
                // capture the error, but continue the loop,
                // to maximize ability to rename everything
                r = Err(e);
            }
        }
        r
    }

    /// Forces a rollover to happen immediately.
    pub fn rollover(&mut self) -> io::Result<()> {
        // Before closing, make sure all data is flushed successfully.
        self.flush()?;
        // We must close the current file before rotating files
        self.writer_opt.take();
        self.current_filesize = 0;
        self.rotate_files()?;
        self.open_writer_if_needed()
    }

    /// Returns a reference to the rolling condition
    pub fn condition_ref(&self) -> &RC {
        &self.condition
    }

    /// Returns a mutable reference to the rolling condition, possibly to mutate its state dynamically.
    pub fn condition_mut(&mut self) -> &mut RC {
        &mut self.condition
    }

    /// Opens a writer for the current file.
    fn open_writer_if_needed(&mut self) -> io::Result<()> {
        if self.writer_opt.is_none() {
            let p = self.filename_for(0);
            let f = OpenOptions::new().append(true).create(true).open(&p)?;
            self.writer_opt = Some(if let Some(capacity) = self.buffer_capacity {
                BufWriter::with_capacity(capacity, f)
            } else {
                BufWriter::new(f)
            });
            self.current_filesize = fs::metadata(&p).map_or(0, |m| m.len());
        }
        Ok(())
    }

    /// Writes data using the given datetime to calculate the rolling condition
    pub fn write_with_datetime(&mut self, buf: &[u8], now: &DateTime<Local>) -> io::Result<usize> {
        if self.condition.should_rollover(now, self.current_filesize) {
            if let Err(e) = self.rollover() {
                // If we can't rollover, just try to continue writing anyway
                // (better than missing data).
                // This will likely used to implement logging, so
                // avoid using log::warn and log to stderr directly
                eprintln!(
                    "WARNING: Failed to rotate logfile {}: {}",
                    self.base_filename.to_string_lossy(),
                    e
                );
            }
        }
        self.open_writer_if_needed()?;
        if let Some(writer) = self.writer_opt.as_mut() {
            let buf_len = buf.len();
            writer.write_all(buf).map(|_| {
                self.current_filesize += u64::try_from(buf_len).unwrap_or(u64::MAX);
                buf_len
            })
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "unexpected condition: writer is missing",
            ))
        }
    }
}

impl<RC> io::Write for RollingFileAppender<RC>
where
    RC: RollingCondition,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let now = Local::now();
        self.write_with_datetime(buf, &now)
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Some(writer) = self.writer_opt.as_mut() {
            writer.flush()?;
        }
        Ok(())
    }
}

/// A rolling file appender with a rolling condition based on date/time or size.
pub type BasicRollingFileAppender = RollingFileAppender<RollingConditionBasic>;

/// A non-rolling file appender.
pub type FileAppender = RollingFileAppender<NoRollingCondition>;

// LCOV_EXCL_START
#[cfg(test)]
mod t {
    use super::*;

    struct Context<RC>
    where
        RC: RollingCondition,
    {
        _tempdir: tempfile::TempDir,
        rolling: RollingFileAppender<RC>,
    }

    impl<RC> Context<RC>
    where
        RC: RollingCondition,
    {
        #[track_caller]
        fn verify_contains(&self, needle: &str, n: usize) {
            let heystack = self.read(n);
            if !heystack.contains(needle) {
                panic!("file {:?} did not contain expected contents {}", self.path(n), needle);
            }
        }

        #[track_caller]
        fn verify_not_contains(&self, needle: &str, n: usize) {
            let heystack = self.read(n);
            if heystack.contains(needle) {
                panic!("file {:?} DID contain expected contents {}", self.path(n), needle);
            }
        }

        fn flush(&mut self) {
            self.rolling.flush().unwrap();
        }

        fn read(&self, n: usize) -> String {
            fs::read_to_string(self.path(n)).unwrap()
        }

        fn path(&self, n: usize) -> OsString {
            self.rolling.filename_for(n)
        }
    }

    fn build_context<RC: RollingCondition>(condition: RC, buffer_capacity: Option<usize>) -> Context<RC> {
        let tempdir = tempfile::tempdir().unwrap();
        let rolling = match buffer_capacity {
            None => RollingFileAppender::<RC>::new(tempdir.path().join("test.log"), condition).unwrap(),
            Some(capacity) => RollingFileAppender::<RC>::new_with_buffer_capacity(
                tempdir.path().join("test.log"),
                condition,
                capacity,
            )
            .unwrap(),
        };
        Context {
            _tempdir: tempdir,
            rolling,
        }
    }

    #[test]
    fn frequency_every_day() {
        let mut c = build_context(RollingConditionBasic::new().daily().max_files(9), None);
        c.rolling
            .write_with_datetime(b"Line 1\n", &Local.with_ymd_and_hms(2021, 3, 30, 1, 2, 3).unwrap())
            .unwrap();
        c.rolling
            .write_with_datetime(b"Line 2\n", &Local.with_ymd_and_hms(2021, 3, 30, 1, 3, 0).unwrap())
            .unwrap();
        c.rolling
            .write_with_datetime(b"Line 3\n", &Local.with_ymd_and_hms(2021, 3, 31, 1, 4, 0).unwrap())
            .unwrap();
        c.rolling
            .write_with_datetime(b"Line 4\n", &Local.with_ymd_and_hms(2021, 5, 31, 1, 4, 0).unwrap())
            .unwrap();
        c.rolling
            .write_with_datetime(b"Line 5\n", &Local.with_ymd_and_hms(2022, 5, 31, 1, 4, 0).unwrap())
            .unwrap();
        assert!(!AsRef::<Path>::as_ref(&c.rolling.filename_for(4)).exists());
        c.flush();
        c.verify_contains("Line 1", 3);
        c.verify_contains("Line 2", 3);
        c.verify_contains("Line 3", 2);
        c.verify_contains("Line 4", 1);
        c.verify_contains("Line 5", 0);
    }

    #[test]
    fn frequency_every_day_limited_files() {
        let mut c = build_context(RollingConditionBasic::new().daily().max_files(2), None);
        c.rolling
            .write_with_datetime(b"Line 1\n", &Local.with_ymd_and_hms(2021, 3, 30, 1, 2, 3).unwrap())
            .unwrap();
        c.rolling
            .write_with_datetime(b"Line 2\n", &Local.with_ymd_and_hms(2021, 3, 30, 1, 3, 0).unwrap())
            .unwrap();
        c.rolling
            .write_with_datetime(b"Line 3\n", &Local.with_ymd_and_hms(2021, 3, 31, 1, 4, 0).unwrap())
            .unwrap();
        c.rolling
            .write_with_datetime(b"Line 4\n", &Local.with_ymd_and_hms(2021, 5, 31, 1, 4, 0).unwrap())
            .unwrap();
        c.rolling
            .write_with_datetime(b"Line 5\n", &Local.with_ymd_and_hms(2022, 5, 31, 1, 4, 0).unwrap())
            .unwrap();
        assert!(!AsRef::<Path>::as_ref(&c.rolling.filename_for(4)).exists());
        assert!(!AsRef::<Path>::as_ref(&c.rolling.filename_for(3)).exists());
        c.flush();
        c.verify_contains("Line 3", 2);
        c.verify_contains("Line 4", 1);
        c.verify_contains("Line 5", 0);
    }

    #[test]
    fn frequency_every_hour() {
        let mut c = build_context(RollingConditionBasic::new().hourly().max_files(9), None);
        c.rolling
            .write_with_datetime(b"Line 1\n", &Local.with_ymd_and_hms(2021, 3, 30, 1, 2, 3).unwrap())
            .unwrap();
        c.rolling
            .write_with_datetime(b"Line 2\n", &Local.with_ymd_and_hms(2021, 3, 30, 1, 3, 2).unwrap())
            .unwrap();
        c.rolling
            .write_with_datetime(b"Line 3\n", &Local.with_ymd_and_hms(2021, 3, 30, 2, 1, 0).unwrap())
            .unwrap();
        c.rolling
            .write_with_datetime(b"Line 4\n", &Local.with_ymd_and_hms(2021, 3, 31, 2, 1, 0).unwrap())
            .unwrap();
        assert!(!AsRef::<Path>::as_ref(&c.rolling.filename_for(3)).exists());
        c.flush();
        c.verify_contains("Line 1", 2);
        c.verify_contains("Line 2", 2);
        c.verify_contains("Line 3", 1);
        c.verify_contains("Line 4", 0);
    }

    #[test]
    fn frequency_every_minute() {
        let mut c = build_context(
            RollingConditionBasic::new()
                .frequency(RollingFrequency::EveryMinute)
                .max_files(9),
            None,
        );
        c.rolling
            .write_with_datetime(b"Line 1\n", &Local.with_ymd_and_hms(2021, 3, 30, 1, 2, 3).unwrap())
            .unwrap();
        c.rolling
            .write_with_datetime(b"Line 2\n", &Local.with_ymd_and_hms(2021, 3, 30, 1, 2, 3).unwrap())
            .unwrap();
        c.rolling
            .write_with_datetime(b"Line 3\n", &Local.with_ymd_and_hms(2021, 3, 30, 1, 2, 4).unwrap())
            .unwrap();
        c.rolling
            .write_with_datetime(b"Line 4\n", &Local.with_ymd_and_hms(2021, 3, 30, 1, 3, 0).unwrap())
            .unwrap();
        c.rolling
            .write_with_datetime(b"Line 5\n", &Local.with_ymd_and_hms(2021, 3, 30, 2, 3, 0).unwrap())
            .unwrap();
        c.rolling
            .write_with_datetime(b"Line 6\n", &Local.with_ymd_and_hms(2022, 3, 30, 2, 3, 0).unwrap())
            .unwrap();
        assert!(!AsRef::<Path>::as_ref(&c.rolling.filename_for(4)).exists());
        c.flush();
        c.verify_contains("Line 1", 3);
        c.verify_contains("Line 2", 3);
        c.verify_contains("Line 3", 3);
        c.verify_contains("Line 4", 2);
        c.verify_contains("Line 5", 1);
        c.verify_contains("Line 6", 0);
    }

    #[test]
    fn max_size() {
        let mut c = build_context(RollingConditionBasic::new().max_size(10).max_files(9), None);
        c.rolling
            .write_with_datetime(b"12345", &Local.with_ymd_and_hms(2021, 3, 30, 1, 2, 3).unwrap())
            .unwrap();
        c.rolling
            .write_with_datetime(b"6789", &Local.with_ymd_and_hms(2021, 3, 30, 1, 3, 3).unwrap())
            .unwrap();
        c.rolling
            .write_with_datetime(b"0", &Local.with_ymd_and_hms(2021, 3, 30, 2, 3, 3).unwrap())
            .unwrap();
        c.rolling
            .write_with_datetime(
                b"abcdefghijklmn",
                &Local.with_ymd_and_hms(2021, 3, 31, 2, 3, 3).unwrap(),
            )
            .unwrap();
        c.rolling
            .write_with_datetime(b"ZZZ", &Local.with_ymd_and_hms(2022, 3, 31, 1, 2, 3).unwrap())
            .unwrap();
        assert!(!AsRef::<Path>::as_ref(&c.rolling.filename_for(3)).exists());
        c.flush();
        c.verify_contains("1234567890", 2);
        c.verify_contains("abcdefghijklmn", 1);
        c.verify_contains("ZZZ", 0);
    }

    #[test]
    fn max_size_existing() {
        let mut c = build_context(RollingConditionBasic::new().max_size(10).max_files(9), None);
        c.rolling
            .write_with_datetime(b"12345", &Local.with_ymd_and_hms(2021, 3, 30, 1, 2, 3).unwrap())
            .unwrap();
        // close the file and make sure that it can re-open it, and that it
        // resets the file size properly.
        c.rolling.writer_opt.take();
        c.rolling.current_filesize = 0;
        c.rolling
            .write_with_datetime(b"6789", &Local.with_ymd_and_hms(2021, 3, 30, 1, 3, 3).unwrap())
            .unwrap();
        c.rolling
            .write_with_datetime(b"0", &Local.with_ymd_and_hms(2021, 3, 30, 2, 3, 3).unwrap())
            .unwrap();
        c.rolling
            .write_with_datetime(
                b"abcdefghijklmn",
                &Local.with_ymd_and_hms(2021, 3, 31, 2, 3, 3).unwrap(),
            )
            .unwrap();
        c.rolling
            .write_with_datetime(b"ZZZ", &Local.with_ymd_and_hms(2022, 3, 31, 1, 2, 3).unwrap())
            .unwrap();
        assert!(!AsRef::<Path>::as_ref(&c.rolling.filename_for(3)).exists());
        c.flush();
        c.verify_contains("1234567890", 2);
        c.verify_contains("abcdefghijklmn", 1);
        c.verify_contains("ZZZ", 0);
    }

    #[test]
    fn daily_and_max_size() {
        let mut c = build_context(RollingConditionBasic::new().daily().max_size(10).max_files(9), None);
        c.rolling
            .write_with_datetime(b"12345", &Local.with_ymd_and_hms(2021, 3, 30, 1, 2, 3).unwrap())
            .unwrap();
        c.rolling
            .write_with_datetime(b"6789", &Local.with_ymd_and_hms(2021, 3, 30, 2, 3, 3).unwrap())
            .unwrap();
        c.rolling
            .write_with_datetime(b"0", &Local.with_ymd_and_hms(2021, 3, 31, 2, 3, 3).unwrap())
            .unwrap();
        c.rolling
            .write_with_datetime(
                b"abcdefghijklmn",
                &Local.with_ymd_and_hms(2021, 3, 31, 3, 3, 3).unwrap(),
            )
            .unwrap();
        c.rolling
            .write_with_datetime(b"ZZZ", &Local.with_ymd_and_hms(2021, 3, 31, 4, 4, 4).unwrap())
            .unwrap();
        assert!(!AsRef::<Path>::as_ref(&c.rolling.filename_for(3)).exists());
        c.flush();
        c.verify_contains("123456789", 2);
        c.verify_contains("0abcdefghijklmn", 1);
        c.verify_contains("ZZZ", 0);
    }

    #[test]
    fn default_buffer_capacity() {
        let c = build_context(RollingConditionBasic::new().daily().max_files(9), None);
        // currently capacity should be 8192; but it may change (ref: https://doc.rust-lang.org/std/io/struct.BufWriter.html#method.new)
        // so we can't hard code and there's no way to get default capacity other than creating a dummy one...
        let default_capacity = BufWriter::new(tempfile::tempfile().unwrap()).capacity();
        if default_capacity != 8192 {
            eprintln!(
                "WARN: it seems std's default capacity is changed from 8192 to {}",
                default_capacity
            );
        }
        assert_eq!(c.rolling.writer_opt.map(|b| b.capacity()), Some(default_capacity));
    }

    #[test]
    fn large_buffer_capacity_and_flush() {
        let mut c = build_context(RollingConditionBasic::new().daily().max_files(9), Some(100_000));
        assert_eq!(c.rolling.writer_opt.as_ref().map(|b| b.capacity()), Some(100_000));
        c.verify_not_contains("12345", 0);

        // implicit flush only after capacity is reached
        loop {
            c.rolling
                .write_with_datetime(b"dummy", &Local.with_ymd_and_hms(2021, 3, 30, 1, 2, 3).unwrap())
                .unwrap();
            if c.rolling.current_filesize <= 100_000 {
                c.verify_not_contains("dummy", 0);
            } else {
                break;
            }
        }
        c.verify_contains("dummy", 0);

        // explicit flush
        c.verify_not_contains("12345", 0);
        c.rolling
            .write_with_datetime(b"12345", &Local.with_ymd_and_hms(2021, 3, 30, 1, 2, 3).unwrap())
            .unwrap();
        c.flush();
        c.verify_contains("12345", 0);
    }

    #[test]
    fn file_appender() {
        let mut c = build_context(NoRollingCondition::default(), Some(100));
        assert_eq!(c.rolling.writer_opt.as_ref().map(|b| b.capacity()), Some(100));

        c.rolling.write_all(b"hello").unwrap();
        c.verify_not_contains("hello", 0);
        c.flush();
        c.verify_contains("hello", 0);

        c.rolling.write_all(b"world").unwrap();
        c.verify_not_contains("world", 0);
        c.flush();
        c.verify_contains("world", 0);
    }
}
// LCOV_EXCL_STOP

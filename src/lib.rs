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
//!     RollingConditionBasic::new().daily(),
//!     9
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
    /// Determine and return whether or not the file should be rolled over.
    fn should_rollover(&mut self, now: &DateTime<Local>, current_filesize: u64) -> bool;
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
            RollingFrequency::EveryDay => Local.ymd(dt.year(), dt.month(), dt.day()).and_hms(0, 0, 0),
            RollingFrequency::EveryHour => Local.ymd(dt.year(), dt.month(), dt.day()).and_hms(dt.hour(), 0, 0),
            RollingFrequency::EveryMinute => {
                Local
                    .ymd(dt.year(), dt.month(), dt.day())
                    .and_hms(dt.hour(), dt.minute(), 0)
            },
        }
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
}

impl RollingConditionBasic {
    /// Constructs a new struct that does not yet have any condition set.
    pub fn new() -> RollingConditionBasic {
        RollingConditionBasic {
            last_write_opt: None,
            frequency_opt: None,
            max_size_opt: None,
        }
    }

    /// Sets a condition to rollover on the given frequency
    pub fn frequency(mut self, x: RollingFrequency) -> RollingConditionBasic {
        self.frequency_opt = Some(x);
        self
    }

    /// Sets a condition to rollover when the date changes
    pub fn daily(mut self) -> RollingConditionBasic {
        self.frequency_opt = Some(RollingFrequency::EveryDay);
        self
    }

    /// Sets a condition to rollover when the date or hour changes
    pub fn hourly(mut self) -> RollingConditionBasic {
        self.frequency_opt = Some(RollingFrequency::EveryHour);
        self
    }

    /// Sets a condition to rollover when a certain size is reached
    pub fn max_size(mut self, x: u64) -> RollingConditionBasic {
        self.max_size_opt = Some(x);
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
    max_files: usize,
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
    pub fn new<P>(path: P, condition: RC, max_files: usize) -> io::Result<RollingFileAppender<RC>>
    where
        P: AsRef<Path>,
    {
        Self::_new(path, condition, max_files, None)
    }

    /// Creates a new rolling file appender with the given condition and write buffer capacity.
    /// The parent directory of the base path must already exist.
    pub fn new_with_buffer_capacity<P>(
        path: P,
        condition: RC,
        max_files: usize,
        buffer_capacity: usize,
    ) -> io::Result<RollingFileAppender<RC>>
    where
        P: AsRef<Path>,
    {
        Self::_new(path, condition, max_files, Some(buffer_capacity))
    }

    fn _new<P>(
        path: P,
        condition: RC,
        max_files: usize,
        buffer_capacity: Option<usize>,
    ) -> io::Result<RollingFileAppender<RC>>
    where
        P: AsRef<Path>,
    {
        let mut rfa = RollingFileAppender {
            condition,
            base_filename: path.as_ref().as_os_str().to_os_string(),
            max_files,
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
        let _ = fs::remove_file(self.filename_for(self.max_files.max(1)));
        let mut r = Ok(());
        for i in (0..self.max_files.max(1)).rev() {
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

// LCOV_EXCL_START
#[cfg(test)]
mod t {
    use super::*;

    struct Context {
        _tempdir: tempfile::TempDir,
        rolling: BasicRollingFileAppender,
    }

    impl Context {
        fn verify_contains(&mut self, needle: &str, n: usize) {
            self.rolling.flush().unwrap();
            let p = self.rolling.filename_for(n);
            let haystack = fs::read_to_string(&p).unwrap();
            if !haystack.contains(needle) {
                panic!("file {:?} did not contain expected contents {}", p, needle);
            }
        }
    }

    fn build_context(condition: RollingConditionBasic, max_files: usize) -> Context {
        let tempdir = tempfile::tempdir().unwrap();
        let rolling = BasicRollingFileAppender::new(tempdir.path().join("test.log"), condition, max_files).unwrap();
        Context {
            _tempdir: tempdir,
            rolling,
        }
    }

    #[test]
    fn frequency_every_day() {
        let mut c = build_context(RollingConditionBasic::new().daily(), 9);
        c.rolling
            .write_with_datetime(b"Line 1\n", &Local.ymd(2021, 3, 30).and_hms(1, 2, 3))
            .unwrap();
        c.rolling
            .write_with_datetime(b"Line 2\n", &Local.ymd(2021, 3, 30).and_hms(1, 3, 0))
            .unwrap();
        c.rolling
            .write_with_datetime(b"Line 3\n", &Local.ymd(2021, 3, 31).and_hms(1, 4, 0))
            .unwrap();
        c.rolling
            .write_with_datetime(b"Line 4\n", &Local.ymd(2021, 5, 31).and_hms(1, 4, 0))
            .unwrap();
        c.rolling
            .write_with_datetime(b"Line 5\n", &Local.ymd(2022, 5, 31).and_hms(1, 4, 0))
            .unwrap();
        assert!(!AsRef::<Path>::as_ref(&c.rolling.filename_for(4)).exists());
        c.verify_contains("Line 1", 3);
        c.verify_contains("Line 2", 3);
        c.verify_contains("Line 3", 2);
        c.verify_contains("Line 4", 1);
        c.verify_contains("Line 5", 0);
    }

    #[test]
    fn frequency_every_day_limited_files() {
        let mut c = build_context(RollingConditionBasic::new().daily(), 2);
        c.rolling
            .write_with_datetime(b"Line 1\n", &Local.ymd(2021, 3, 30).and_hms(1, 2, 3))
            .unwrap();
        c.rolling
            .write_with_datetime(b"Line 2\n", &Local.ymd(2021, 3, 30).and_hms(1, 3, 0))
            .unwrap();
        c.rolling
            .write_with_datetime(b"Line 3\n", &Local.ymd(2021, 3, 31).and_hms(1, 4, 0))
            .unwrap();
        c.rolling
            .write_with_datetime(b"Line 4\n", &Local.ymd(2021, 5, 31).and_hms(1, 4, 0))
            .unwrap();
        c.rolling
            .write_with_datetime(b"Line 5\n", &Local.ymd(2022, 5, 31).and_hms(1, 4, 0))
            .unwrap();
        assert!(!AsRef::<Path>::as_ref(&c.rolling.filename_for(4)).exists());
        assert!(!AsRef::<Path>::as_ref(&c.rolling.filename_for(3)).exists());
        c.verify_contains("Line 3", 2);
        c.verify_contains("Line 4", 1);
        c.verify_contains("Line 5", 0);
    }

    #[test]
    fn frequency_every_hour() {
        let mut c = build_context(RollingConditionBasic::new().hourly(), 9);
        c.rolling
            .write_with_datetime(b"Line 1\n", &Local.ymd(2021, 3, 30).and_hms(1, 2, 3))
            .unwrap();
        c.rolling
            .write_with_datetime(b"Line 2\n", &Local.ymd(2021, 3, 30).and_hms(1, 3, 2))
            .unwrap();
        c.rolling
            .write_with_datetime(b"Line 3\n", &Local.ymd(2021, 3, 30).and_hms(2, 1, 0))
            .unwrap();
        c.rolling
            .write_with_datetime(b"Line 4\n", &Local.ymd(2021, 3, 31).and_hms(2, 1, 0))
            .unwrap();
        assert!(!AsRef::<Path>::as_ref(&c.rolling.filename_for(3)).exists());
        c.verify_contains("Line 1", 2);
        c.verify_contains("Line 2", 2);
        c.verify_contains("Line 3", 1);
        c.verify_contains("Line 4", 0);
    }

    #[test]
    fn frequency_every_minute() {
        let mut c = build_context(RollingConditionBasic::new().frequency(RollingFrequency::EveryMinute), 9);
        c.rolling
            .write_with_datetime(b"Line 1\n", &Local.ymd(2021, 3, 30).and_hms(1, 2, 3))
            .unwrap();
        c.rolling
            .write_with_datetime(b"Line 2\n", &Local.ymd(2021, 3, 30).and_hms(1, 2, 3))
            .unwrap();
        c.rolling
            .write_with_datetime(b"Line 3\n", &Local.ymd(2021, 3, 30).and_hms(1, 2, 4))
            .unwrap();
        c.rolling
            .write_with_datetime(b"Line 4\n", &Local.ymd(2021, 3, 30).and_hms(1, 3, 0))
            .unwrap();
        c.rolling
            .write_with_datetime(b"Line 5\n", &Local.ymd(2021, 3, 30).and_hms(2, 3, 0))
            .unwrap();
        c.rolling
            .write_with_datetime(b"Line 6\n", &Local.ymd(2022, 3, 30).and_hms(2, 3, 0))
            .unwrap();
        assert!(!AsRef::<Path>::as_ref(&c.rolling.filename_for(4)).exists());
        c.verify_contains("Line 1", 3);
        c.verify_contains("Line 2", 3);
        c.verify_contains("Line 3", 3);
        c.verify_contains("Line 4", 2);
        c.verify_contains("Line 5", 1);
        c.verify_contains("Line 6", 0);
    }

    #[test]
    fn max_size() {
        let mut c = build_context(RollingConditionBasic::new().max_size(10), 9);
        c.rolling
            .write_with_datetime(b"12345", &Local.ymd(2021, 3, 30).and_hms(1, 2, 3))
            .unwrap();
        c.rolling
            .write_with_datetime(b"6789", &Local.ymd(2021, 3, 30).and_hms(1, 3, 3))
            .unwrap();
        c.rolling
            .write_with_datetime(b"0", &Local.ymd(2021, 3, 30).and_hms(2, 3, 3))
            .unwrap();
        c.rolling
            .write_with_datetime(b"abcdefghijklmn", &Local.ymd(2021, 3, 31).and_hms(2, 3, 3))
            .unwrap();
        c.rolling
            .write_with_datetime(b"ZZZ", &Local.ymd(2022, 3, 31).and_hms(1, 2, 3))
            .unwrap();
        assert!(!AsRef::<Path>::as_ref(&c.rolling.filename_for(3)).exists());
        c.verify_contains("1234567890", 2);
        c.verify_contains("abcdefghijklmn", 1);
        c.verify_contains("ZZZ", 0);
    }

    #[test]
    fn max_size_existing() {
        let mut c = build_context(RollingConditionBasic::new().max_size(10), 9);
        c.rolling
            .write_with_datetime(b"12345", &Local.ymd(2021, 3, 30).and_hms(1, 2, 3))
            .unwrap();
        // close the file and make sure that it can re-open it, and that it
        // resets the file size properly.
        c.rolling.writer_opt.take();
        c.rolling.current_filesize = 0;
        c.rolling
            .write_with_datetime(b"6789", &Local.ymd(2021, 3, 30).and_hms(1, 3, 3))
            .unwrap();
        c.rolling
            .write_with_datetime(b"0", &Local.ymd(2021, 3, 30).and_hms(2, 3, 3))
            .unwrap();
        c.rolling
            .write_with_datetime(b"abcdefghijklmn", &Local.ymd(2021, 3, 31).and_hms(2, 3, 3))
            .unwrap();
        c.rolling
            .write_with_datetime(b"ZZZ", &Local.ymd(2022, 3, 31).and_hms(1, 2, 3))
            .unwrap();
        assert!(!AsRef::<Path>::as_ref(&c.rolling.filename_for(3)).exists());
        c.verify_contains("1234567890", 2);
        c.verify_contains("abcdefghijklmn", 1);
        c.verify_contains("ZZZ", 0);
    }

    #[test]
    fn daily_and_max_size() {
        let mut c = build_context(RollingConditionBasic::new().daily().max_size(10), 9);
        c.rolling
            .write_with_datetime(b"12345", &Local.ymd(2021, 3, 30).and_hms(1, 2, 3))
            .unwrap();
        c.rolling
            .write_with_datetime(b"6789", &Local.ymd(2021, 3, 30).and_hms(2, 3, 3))
            .unwrap();
        c.rolling
            .write_with_datetime(b"0", &Local.ymd(2021, 3, 31).and_hms(2, 3, 3))
            .unwrap();
        c.rolling
            .write_with_datetime(b"abcdefghijklmn", &Local.ymd(2021, 3, 31).and_hms(3, 3, 3))
            .unwrap();
        c.rolling
            .write_with_datetime(b"ZZZ", &Local.ymd(2021, 3, 31).and_hms(4, 4, 4))
            .unwrap();
        assert!(!AsRef::<Path>::as_ref(&c.rolling.filename_for(3)).exists());
        c.verify_contains("123456789", 2);
        c.verify_contains("0abcdefghijklmn", 1);
        c.verify_contains("ZZZ", 0);
    }
}
// LCOV_EXCL_STOP

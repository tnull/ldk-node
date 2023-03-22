#[cfg(target_os = "windows")]
extern crate winapi;

use super::{KVStore, KVStoreUnpersister, TransactionalWrite};

use std::fs;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;

#[cfg(not(target_os = "windows"))]
use std::os::unix::io::AsRawFd;

use lightning::util::persist::KVStorePersister;
use lightning::util::ser::Writeable;

#[cfg(target_os = "windows")]
use {std::ffi::OsStr, std::os::windows::ffi::OsStrExt};

#[cfg(target_os = "windows")]
macro_rules! call {
	($e: expr) => {
		if $e != 0 {
			return Ok(());
		} else {
			return Err(std::io::Error::last_os_error());
		}
	};
}

#[cfg(target_os = "windows")]
fn path_to_windows_str<T: AsRef<OsStr>>(path: T) -> Vec<winapi::shared::ntdef::WCHAR> {
	path.as_ref().encode_wide().chain(Some(0)).collect()
}

pub struct FilesystemStore {
	dest_dir: PathBuf,
}

impl FilesystemStore {
	pub fn new(dest_dir: PathBuf) -> Self {
		Self { dest_dir }
	}
}

impl KVStore<FilesystemReader, FilesystemWriter> for FilesystemStore {
	fn read(&self, namespace: &str, key: &str) -> std::io::Result<FilesystemReader> {
		let mut dest_file = self.dest_dir.clone();
		dest_file.push(namespace);
		dest_file.push(key);
		FilesystemReader::new(dest_file)
	}

	fn write(&self, namespace: &str, key: &str) -> std::io::Result<FilesystemWriter> {
		let mut dest_file = self.dest_dir.clone();
		dest_file.push(namespace);
		dest_file.push(key);
		FilesystemWriter::new(dest_file)
	}

	fn remove(&self, namespace: &str, key: &str) -> std::io::Result<bool> {
		let mut dest_file = self.dest_dir.clone();
		dest_file.push(namespace);
		dest_file.push(key);

		if !dest_file.is_file() {
			return Ok(false);
		}

		fs::remove_file(&dest_file)?;
		#[cfg(not(target_os = "windows"))]
		{
			let msg = format!("Could not retrieve parent directory of {}.", dest_file.display());
			let parent_directory = dest_file
				.parent()
				.ok_or(std::io::Error::new(std::io::ErrorKind::InvalidInput, msg))?;
			let dir_file = fs::OpenOptions::new().read(true).open(parent_directory)?;
			unsafe {
				// The above call to `fs::remove_file` corresponds to POSIX `unlink`, whose changes
				// to the inode might get cached (and hence possibly lost on crash), depending on
				// the target platform and file system.
				//
				// In order to assert we permanently removed the file in question we therefore
				// call `fsync` on the parent directory on platforms that support it,
				libc::fsync(dir_file.as_raw_fd());
			}
		}

		if dest_file.is_file() {
			return Err(std::io::Error::new(std::io::ErrorKind::Other, "Unpersisting key failed"));
		}

		Ok(true)
	}

	fn list(&self, namespace: &str) -> std::io::Result<Vec<String>> {
		let mut prefixed_dest = self.dest_dir.clone();
		prefixed_dest.push(namespace);

		let mut keys = Vec::new();

		if !Path::new(&prefixed_dest).exists() {
			return Ok(Vec::new());
		}

		for entry in fs::read_dir(prefixed_dest.clone())? {
			let entry = entry?;
			let p = entry.path();

			if !p.is_file() {
				continue;
			}

			if let Some(ext) = p.extension() {
				if ext == "tmp" {
					continue;
				}
			}

			if let Ok(relative_path) = p.strip_prefix(prefixed_dest.clone()) {
				keys.push(relative_path.display().to_string())
			}
		}

		Ok(keys)
	}
}

pub struct FilesystemReader {
	inner: BufReader<fs::File>,
}

impl FilesystemReader {
	pub fn new(dest_file: PathBuf) -> std::io::Result<Self> {
		let f = fs::File::open(dest_file.clone())?;
		let inner = BufReader::new(f);
		Ok(Self { inner })
	}
}

impl Read for FilesystemReader {
	fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
		self.inner.read(buf)
	}
}

pub struct FilesystemWriter {
	dest_file: PathBuf,
	parent_directory: PathBuf,
	tmp_file: PathBuf,
	tmp_writer: BufWriter<fs::File>,
}

impl FilesystemWriter {
	pub fn new(dest_file: PathBuf) -> std::io::Result<Self> {
		let msg = format!("Could not retrieve parent directory of {}.", dest_file.display());
		let parent_directory = dest_file
			.parent()
			.ok_or(std::io::Error::new(std::io::ErrorKind::InvalidInput, msg))?
			.to_path_buf();
		fs::create_dir_all(parent_directory.clone())?;

		// Do a crazy dance with lots of fsync()s to be overly cautious here...
		// We never want to end up in a state where we've lost the old data, or end up using the
		// old data on power loss after we've returned.
		// The way to atomically write a file on Unix platforms is:
		// open(tmpname), write(tmpfile), fsync(tmpfile), close(tmpfile), rename(), fsync(dir)
		let mut tmp_file = dest_file.clone();
		tmp_file.set_extension("tmp");

		let tmp_writer = BufWriter::new(fs::File::create(&tmp_file)?);

		Ok(Self { dest_file, parent_directory, tmp_file, tmp_writer })
	}
}

impl Write for FilesystemWriter {
	fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
		Ok(self.tmp_writer.write(buf)?)
	}

	fn flush(&mut self) -> std::io::Result<()> {
		self.tmp_writer.flush()?;
		self.tmp_writer.get_ref().sync_all()?;
		Ok(())
	}
}

impl TransactionalWrite for FilesystemWriter {
	fn commit(&mut self) -> std::io::Result<()> {
		self.flush()?;
		// Fsync the parent directory on Unix.
		#[cfg(not(target_os = "windows"))]
		{
			fs::rename(&self.tmp_file, &self.dest_file)?;
			let dir_file = fs::OpenOptions::new().read(true).open(self.parent_directory.clone())?;
			unsafe {
				libc::fsync(dir_file.as_raw_fd());
			}
		}

		#[cfg(target_os = "windows")]
		{
			if dest_file.exists() {
				unsafe {
					winapi::um::winbase::ReplaceFileW(
						path_to_windows_str(dest_file).as_ptr(),
						path_to_windows_str(tmp_file).as_ptr(),
						std::ptr::null(),
						winapi::um::winbase::REPLACEFILE_IGNORE_MERGE_ERRORS,
						std::ptr::null_mut() as *mut winapi::ctypes::c_void,
						std::ptr::null_mut() as *mut winapi::ctypes::c_void,
					)
				};
			} else {
				call!(unsafe {
					winapi::um::winbase::MoveFileExW(
						path_to_windows_str(tmp_file).as_ptr(),
						path_to_windows_str(dest_file).as_ptr(),
						winapi::um::winbase::MOVEFILE_WRITE_THROUGH
							| winapi::um::winbase::MOVEFILE_REPLACE_EXISTING,
					)
				});
			}
		}
		Ok(())
	}
}

impl KVStorePersister for FilesystemStore {
	fn persist<W: Writeable>(&self, prefixed_key: &str, object: &W) -> lightning::io::Result<()> {
		let msg = format!("Could not persist file for key {}.", prefixed_key);
		let dest_file = PathBuf::from_str(prefixed_key).map_err(|_| {
			lightning::io::Error::new(lightning::io::ErrorKind::InvalidInput, msg.clone())
		})?;

		let parent_directory = dest_file.parent().ok_or(lightning::io::Error::new(
			lightning::io::ErrorKind::InvalidInput,
			msg.clone(),
		))?;
		let namespace = parent_directory.display().to_string();

		let dest_without_namespace = dest_file
			.strip_prefix(&namespace)
			.map_err(|_| lightning::io::Error::new(lightning::io::ErrorKind::InvalidInput, msg))?;
		let key = dest_without_namespace.display().to_string();
		let mut writer = self.write(&namespace, &key)?;
		object.write(&mut writer)?;
		Ok(writer.commit()?)
	}
}

impl KVStoreUnpersister for FilesystemStore {
	fn unpersist(&self, key: &str) -> std::io::Result<bool> {
		let msg = format!("Could not retrieve file for key {}.", key);
		let dest_file = PathBuf::from_str(key)
			.map_err(|_| lightning::io::Error::new(lightning::io::ErrorKind::InvalidInput, msg))?;
		let msg = format!("Could not retrieve parent directory of {}.", dest_file.display());
		let parent_directory = dest_file
			.parent()
			.ok_or(lightning::io::Error::new(lightning::io::ErrorKind::InvalidInput, msg))?;
		let namespace = parent_directory.display().to_string();
		self.remove(&namespace, key)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::test::utils::random_storage_path;
	use lightning::util::persist::KVStorePersister;
	use lightning::util::ser::{Readable, Writeable};

	use proptest::prelude::*;
	proptest! {
		#[test]
		fn read_write_remove_list_persist(data in any::<[u8; 32]>()) {
			let rand_dir = random_storage_path();

			let fs_store = FilesystemStore::new(rand_dir.into());
			let namespace = "testspace";
			let key = "testkey";

			// Test the basic KVStore operations.
			let mut writer = fs_store.write(namespace, key).unwrap();
			data.write(&mut writer).unwrap();
			writer.commit().unwrap();

			let listed_keys = fs_store.list(namespace).unwrap();
			assert_eq!(listed_keys.len(), 1);
			assert_eq!(listed_keys[0], "testkey");

			let mut reader = fs_store.read(namespace, key).unwrap();
			let read_data: [u8; 32] = Readable::read(&mut reader).unwrap();
			assert_eq!(data, read_data);

			fs_store.remove(namespace, key).unwrap();

			let listed_keys = fs_store.list(namespace).unwrap();
			assert_eq!(listed_keys.len(), 0);

			// Test KVStorePersister
			let prefixed_key = format!("{}/{}", namespace, key);
			fs_store.persist(&prefixed_key, &data).unwrap();
			let mut reader = fs_store.read(namespace, key).unwrap();
			let read_data: [u8; 32] = Readable::read(&mut reader).unwrap();
			assert_eq!(data, read_data);
		}
	}
}

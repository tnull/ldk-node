pub(crate) mod fs_store;
pub(crate) mod utils;

use lightning::chain::channelmonitor::ChannelMonitor;
use lightning::chain::keysinterface::{EntropySource, SignerProvider};
use lightning::util::ser::ReadableArgs;

use bitcoin::hash_types::{BlockHash, Txid};
use bitcoin::hashes::hex::FromHex;

use std::io::{Read, Write};
use std::ops::Deref;

// The namespace and key LDK uses for persisting
pub(crate) const CHANNEL_MANAGER_PERSISTENCE_KEY: &str = "manager";
pub(crate) const CHANNEL_MANAGER_PERSISTENCE_NAMESPACE: &str = "";

pub(crate) const CHANNEL_MONITOR_PERSISTENCE_NAMESPACE: &str = "monitors";

/// Provides an interface that allows to store and retrieve persisted values that are associated
/// with given keys.
///
/// In order to avoid collisions the key space is segmented based on the given `namespace`s.
/// Implementations of this trait are free to handle them in different ways, as long as
/// per-namespace key uniqueness is asserted.
///
/// Keys and namespaces are required to be valid ASCII strings and the empty namespace (`""`) is
/// assumed to be valid namespace.
pub trait KVStore<R: Read, TW: TransactionalWrite> {
	/// Returns a [`Read`] for the given `key` from which [`Readable`]s may be read.
	///
	/// [`Readable`]: lightning::util::ser::Readable
	fn read(&self, namespace: &str, key: &str) -> std::io::Result<R>;
	/// Returns a [`TransactionalWrite`] for the given `key` to which [`Writeable`]s may be written.
	///
	/// Note that [`TransactionalWrite::commit`] MUST be called to commit the written data, otherwise
	/// the changes won't be persisted.
	///
	/// [`Writeable`]: lightning::util::ser::Writeable
	fn write(&self, namespace: &str, key: &str) -> std::io::Result<TW>;
	/// Removes any data that had previously been persisted under the given `key`.
	///
	/// Returns `true` if the key was present, and `false` otherwise.
	fn remove(&self, namespace: &str, key: &str) -> std::io::Result<bool>;
	/// Returns a list of keys that are stored under the given `namespace`.
	fn list(&self, namespace: &str) -> std::io::Result<Vec<String>>;
	/// Read the persisted [`ChannelMonitor`]s from the store.
	fn read_channelmonitors<ES: Deref, SP: Deref>(
		&self, entropy_source: ES, signer_provider: SP,
	) -> std::io::Result<Vec<(BlockHash, ChannelMonitor<<SP::Target as SignerProvider>::Signer>)>>
	where
		ES::Target: EntropySource + Sized,
		SP::Target: SignerProvider + Sized,
	{
		let mut res = Vec::new();

		for stored_key in self.list(CHANNEL_MONITOR_PERSISTENCE_NAMESPACE)? {
			let txid = Txid::from_hex(stored_key.split_at(64).0).map_err(|_| {
				std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid tx ID in stored key")
			})?;

			let index: u16 = stored_key.split_at(65).1.parse().map_err(|_| {
				std::io::Error::new(
					std::io::ErrorKind::InvalidData,
					"Invalid tx index in stored key",
				)
			})?;

			match <(BlockHash, ChannelMonitor<<SP::Target as SignerProvider>::Signer>)>::read(
				&mut self.read(CHANNEL_MONITOR_PERSISTENCE_NAMESPACE, &stored_key)?,
				(&*entropy_source, &*signer_provider),
			) {
				Ok((block_hash, channel_monitor)) => {
					if channel_monitor.get_funding_txo().0.txid != txid
						|| channel_monitor.get_funding_txo().0.index != index
					{
						return Err(std::io::Error::new(
							std::io::ErrorKind::InvalidData,
							"ChannelMonitor was stored under the wrong key",
						));
					}
					res.push((block_hash, channel_monitor));
				}
				Err(e) => {
					return Err(std::io::Error::new(
						std::io::ErrorKind::InvalidData,
						format!("Failed to deserialize ChannelMonitor: {}", e),
					))
				}
			}
		}
		Ok(res)
	}
}

/// A [`Write`] asserting data consistency.
///
/// Note that any changes need to be `commit`ed for them to take effect, and are lost otherwise.
pub trait TransactionalWrite: Write {
	/// Persist the previously made changes.
	fn commit(&mut self) -> std::io::Result<()>;
}

/// Provides an interface that allows a previously persisted key to be unpersisted.
pub trait KVStoreUnpersister {
	/// Unpersist (i.e., remove) the writeable previously persisted under the provided key.
	/// Returns `true` if the key was present, and `false` otherwise.
	fn unpersist(&self, key: &str) -> std::io::Result<bool>;
}

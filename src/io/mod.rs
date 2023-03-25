pub(crate) mod fs_store;
pub(crate) mod utils;

use std::io::{Read, Write};

// The namespacs and keys LDK uses for persisting
pub(crate) const CHANNEL_MANAGER_PERSISTENCE_NAMESPACE: &str = "";
pub(crate) const CHANNEL_MANAGER_PERSISTENCE_KEY: &str = "manager";

pub(crate) const CHANNEL_MONITOR_PERSISTENCE_NAMESPACE: &str = "monitors";

pub(crate) const NETWORK_GRAPH_PERSISTENCE_NAMESPACE: &str = "";
pub(crate) const NETWORK_GRAPH_PERSISTENCE_KEY: &str = "network_graph";

pub(crate) const SCORER_PERSISTENCE_NAMESPACE: &str = "";
pub(crate) const SCORER_PERSISTENCE_KEY: &str = "scorer";

/// The event queue will be persisted under this key.
pub(crate) const EVENT_QUEUE_PERSISTENCE_NAMESPACE: &str = "";
pub(crate) const EVENT_QUEUE_PERSISTENCE_KEY: &str = "events";

/// The peer information will be persisted under this key.
pub(crate) const PEER_INFO_PERSISTENCE_NAMESPACE: &str = "";
pub(crate) const PEER_INFO_PERSISTENCE_KEY: &str = "peers";

/// The payment information will be persisted under this prefix.
pub(crate) const PAYMENT_INFO_PERSISTENCE_NAMESPACE: &str = "payments";

/// Provides an interface that allows to store and retrieve persisted values that are associated
/// with given keys.
///
/// In order to avoid collisions the key space is segmented based on the given `namespace`s.
/// Implementations of this trait are free to handle them in different ways, as long as
/// per-namespace key uniqueness is asserted.
///
/// Keys and namespaces are required to be valid ASCII strings and the empty namespace (`""`) is
/// assumed to be valid namespace.
pub trait KVStore {
	/// Returns a [`Read`] for the given `namespace` and `key` from which [`Readable`]s may be
	/// read.
	///
	/// Returns an `Err` if the given `key` could not be found in the given `namespace`.
	///
	/// [`Readable`]: lightning::util::ser::Readable
	fn read(&self, namespace: &str, key: &str) -> std::io::Result<Box<dyn Read>>;
	/// Returns a [`TransactionalWrite`] for the given `key` to which [`Writeable`]s may be written.
	///
	/// Will create the given `namespace` if not already present in the store.
	///
	/// Note that [`TransactionalWrite::commit`] MUST be called to commit the written data, otherwise
	/// the changes won't be persisted.
	///
	/// [`Writeable`]: lightning::util::ser::Writeable
	fn write(&self, namespace: &str, key: &str) -> std::io::Result<Box<dyn TransactionalWrite>>;
	/// Removes any data that had previously been persisted under the given `key`.
	///
	/// Returns `true` if the `key` was present in the given `namespace`, and `false` otherwise.
	fn remove(&self, namespace: &str, key: &str) -> std::io::Result<bool>;
	/// Returns a list of keys that are stored under the given `namespace`.
	///
	/// Will return an empty list if the `namespace` is unknown.
	fn list(&self, namespace: &str) -> std::io::Result<Vec<String>>;
}

/// A [`Write`] asserting data consistency.
///
/// Note that any changes need to be `commit`ed for them to take effect, and are lost otherwise.
pub trait TransactionalWrite: Write {
	/// Persist the previously made changes.
	fn commit(&mut self) -> std::io::Result<()>;
}

// TODO: Use this generic impl once we have Node<K>
//impl<T> KVStorePersister for T
//where T: KVStore
//{
//	fn persist<W: Writeable>(&self, prefixed_key: &str, object: &W) -> lightning::io::Result<()> {
//		let msg = format!("Could not persist data for key {}.", prefixed_key);
//		let dest_file = PathBuf::from_str(prefixed_key).map_err(|_| {
//			lightning::io::Error::new(lightning::io::ErrorKind::InvalidInput, msg.clone())
//		})?;
//
//		let parent_directory = dest_file.parent().ok_or(lightning::io::Error::new(
//			lightning::io::ErrorKind::InvalidInput,
//			msg.clone(),
//		))?;
//		let namespace = parent_directory.display().to_string();
//
//		let dest_without_namespace = dest_file
//			.strip_prefix(&namespace)
//			.map_err(|_| lightning::io::Error::new(lightning::io::ErrorKind::InvalidInput, msg))?;
//		let key = dest_without_namespace.display().to_string();
//		let mut writer = self.write(&namespace, &key)?;
//		object.write(&mut writer)?;
//		Ok(writer.commit()?)
//	}
//}

/// Provides an interface that allows a previously persisted key to be unpersisted.
pub trait KVStoreUnpersister {
	/// Unpersist (i.e., remove) the writeable previously persisted under the provided key.
	/// Returns `true` if the key was present, and `false` otherwise.
	fn unpersist(&self, key: &str) -> std::io::Result<bool>;
}

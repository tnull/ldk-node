use crate::hex_utils;
use crate::io::{KVStore, PAYMENT_INFO_PERSISTENCE_NAMESPACE};
use crate::logger::{log_error, Logger};
use crate::Error;

use lightning::ln::{PaymentHash, PaymentPreimage, PaymentSecret};
use lightning::util::ser::Writeable;
use lightning::{impl_writeable_tlv_based, impl_writeable_tlv_based_enum};

use std::collections::hash_map;
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use std::ops::Deref;
use std::sync::{Mutex, MutexGuard};

/// Represents a payment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PaymentInfo {
	/// The payment hash, i.e., the hash of the `preimage`.
	pub payment_hash: PaymentHash,
	/// The pre-image used by the payment.
	pub preimage: Option<PaymentPreimage>,
	/// The secret used by the payment.
	pub secret: Option<PaymentSecret>,
	/// The amount transferred.
	pub amount_msat: Option<u64>,
	/// The direction of the payment.
	pub direction: PaymentDirection,
	/// The status of the payment.
	pub status: PaymentStatus,
}

impl_writeable_tlv_based!(PaymentInfo, {
	(0, payment_hash, required),
	(1, preimage, required),
	(2, secret, required),
	(3, amount_msat, required),
	(4, direction, required),
	(5, status, required)
});

/// Represents the direction of a payment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PaymentDirection {
	/// The payment is inbound.
	Inbound,
	/// The payment is outbound.
	Outbound,
}

impl_writeable_tlv_based_enum!(PaymentDirection,
	(0, Inbound) => {},
	(1, Outbound) => {};
);

/// Represents the current status of a payment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PaymentStatus {
	/// The payment is still pending.
	Pending,
	/// The payment suceeded.
	Succeeded,
	/// The payment failed.
	Failed,
}

impl_writeable_tlv_based_enum!(PaymentStatus,
	(0, Pending) => {},
	(1, Succeeded) => {},
	(2, Failed) => {};
);

pub(crate) struct PaymentInfoStorage<K: Deref + Clone, L: Deref>
where
	K::Target: KVStore,
	L::Target: Logger,
{
	payments: Mutex<HashMap<PaymentHash, PaymentInfo>>,
	kv_store: K,
	logger: L,
}

impl<K: Deref + Clone, L: Deref> PaymentInfoStorage<K, L>
where
	K::Target: KVStore,
	L::Target: Logger,
{
	pub(crate) fn new(kv_store: K, logger: L) -> Self {
		let payments = Mutex::new(HashMap::new());
		Self { payments, kv_store, logger }
	}

	pub(crate) fn from_payments(payments: Vec<PaymentInfo>, kv_store: K, logger: L) -> Self {
		let payments = Mutex::new(HashMap::from_iter(
			payments.into_iter().map(|payment_info| (payment_info.payment_hash, payment_info)),
		));
		Self { payments, kv_store, logger }
	}

	pub(crate) fn insert(&self, payment_info: PaymentInfo) -> Result<(), Error> {
		let mut locked_payments = self.payments.lock().unwrap();

		let payment_hash = payment_info.payment_hash.clone();
		locked_payments.insert(payment_hash.clone(), payment_info.clone());
		self.write_info_and_commit(&payment_hash, &payment_info)
	}

	pub(crate) fn lock(&self) -> Result<PaymentInfoGuard<K>, ()> {
		let locked_store = self.payments.lock().map_err(|_| ())?;
		Ok(PaymentInfoGuard::new(locked_store, self.kv_store.clone()))
	}

	pub(crate) fn remove(&self, payment_hash: &PaymentHash) -> Result<(), Error> {
		let store_key = hex_utils::to_string(&payment_hash.0);
		self.kv_store.remove(PAYMENT_INFO_PERSISTENCE_NAMESPACE, &store_key).map_err(|e| {
			log_error!(
				self.logger,
				"Removing payment data for key {}/{} failed due to: {}",
				PAYMENT_INFO_PERSISTENCE_NAMESPACE,
				store_key,
				e
			);
			Error::PersistenceFailed
		})?;
		Ok(())
	}

	pub(crate) fn get(&self, payment_hash: &PaymentHash) -> Option<PaymentInfo> {
		self.payments.lock().unwrap().get(payment_hash).cloned()
	}

	pub(crate) fn contains(&self, payment_hash: &PaymentHash) -> bool {
		self.payments.lock().unwrap().contains_key(payment_hash)
	}

	pub(crate) fn set_status(
		&self, payment_hash: &PaymentHash, payment_status: PaymentStatus,
	) -> Result<(), Error> {
		let mut locked_payments = self.payments.lock().unwrap();

		if let Some(payment_info) = locked_payments.get_mut(payment_hash) {
			payment_info.status = payment_status;
			self.write_info_and_commit(payment_hash, payment_info)?;
		}
		Ok(())
	}

	fn write_info_and_commit(
		&self, payment_hash: &PaymentHash, payment_info: &PaymentInfo,
	) -> Result<(), Error> {
		let store_key = hex_utils::to_string(&payment_hash.0);
		let mut writer =
			self.kv_store.write(PAYMENT_INFO_PERSISTENCE_NAMESPACE, &store_key).map_err(|e| {
				log_error!(
					self.logger,
					"Getting writer for key {}/{} failed due to: {}",
					PAYMENT_INFO_PERSISTENCE_NAMESPACE,
					store_key,
					e
				);
				Error::PersistenceFailed
			})?;
		payment_info.write(&mut writer).map_err(|e| {
			log_error!(
				self.logger,
				"Writing payment data for key {}/{} failed due to: {}",
				PAYMENT_INFO_PERSISTENCE_NAMESPACE,
				store_key,
				e
			);
			Error::PersistenceFailed
		})?;
		writer.commit().map_err(|e| {
			log_error!(
				self.logger,
				"Committing payment data for key {}/{} failed due to: {}",
				PAYMENT_INFO_PERSISTENCE_NAMESPACE,
				store_key,
				e
			);
			Error::PersistenceFailed
		})?;
		Ok(())
	}
}

pub(crate) struct PaymentInfoGuard<'a, K: Deref>
where
	K::Target: KVStore,
{
	inner: MutexGuard<'a, HashMap<PaymentHash, PaymentInfo>>,
	touched_keys: HashSet<PaymentHash>,
	kv_store: K,
}

impl<'a, K: Deref> PaymentInfoGuard<'a, K>
where
	K::Target: KVStore,
{
	pub fn new(inner: MutexGuard<'a, HashMap<PaymentHash, PaymentInfo>>, kv_store: K) -> Self {
		let touched_keys = HashSet::new();
		Self { inner, touched_keys, kv_store }
	}

	pub fn entry(
		&mut self, payment_hash: PaymentHash,
	) -> hash_map::Entry<PaymentHash, PaymentInfo> {
		self.touched_keys.insert(payment_hash);
		self.inner.entry(payment_hash)
	}
}

impl<'a, K: Deref> Drop for PaymentInfoGuard<'a, K>
where
	K::Target: KVStore,
{
	fn drop(&mut self) {
		for key in self.touched_keys.iter() {
			let store_key = hex_utils::to_string(&key.0);

			match self.inner.entry(*key) {
				hash_map::Entry::Vacant(_) => {
					self.kv_store
						.remove(PAYMENT_INFO_PERSISTENCE_NAMESPACE, &store_key)
						.expect("Persistence failed");
				}
				hash_map::Entry::Occupied(e) => {
					let mut writer = self
						.kv_store
						.write(PAYMENT_INFO_PERSISTENCE_NAMESPACE, &store_key)
						.expect("Persistence failed");
					e.get().write(&mut writer).expect("Persistence failed");
					writer.commit().expect("Persistence failed");
				}
			};
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::test::utils::{TestLogger, TestStore};
	use std::sync::Arc;

	#[test]
	fn persistence_guard_persists_on_drop() {
		let store = Arc::new(TestStore::new());
		let logger = Arc::new(TestLogger::new());
		let payment_info_store = PaymentInfoStorage::new(Arc::clone(&store), logger);

		let payment_hash = PaymentHash([42u8; 32]);
		assert!(!payment_info_store.contains(&payment_hash));

		let payment_info = PaymentInfo {
			payment_hash,
			preimage: None,
			secret: None,
			amount_msat: None,
			direction: PaymentDirection::Inbound,
			status: PaymentStatus::Pending,
		};

		assert!(!store.get_and_clear_did_persist());
		payment_info_store.lock().unwrap().entry(payment_hash).or_insert(payment_info);
		assert!(store.get_and_clear_did_persist());
	}
}

// This file is Copyright its original authors, visible in version control history.
//
// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. You may not use this file except in
// accordance with one or both of these licenses.

//! BDK-integrated wallet for swap-in-potentiam address management and UTXO tracking.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use bitcoin::bip32::{ChildNumber, Xpriv};
use bitcoin::secp256k1::{PublicKey, Secp256k1, SecretKey};
use bitcoin::{Address, Amount, Network, OutPoint, Transaction};

use lightning_liquidity::sip::address::{
	build_sip_witness_script, cooperative_spend_satisfaction_weight, sip_p2wsh_address,
};

use crate::logger::{log_debug, log_info, log_trace, LdkLogger, Logger};
use crate::sip::state::{SipUtxo, SipUtxoInfo, SipUtxoState};

/// The BIP32 hardened derivation index for swap-in-potentiam keys.
///
/// Keys are derived at `m/sip_key'/<index>` where `sip_key'` is this hardened index
/// and `<index>` is an incrementing non-hardened index for each new SIP address.
const SIP_HARDENED_CHILD_INDEX: u32 = 787;

/// Information about a generated SIP address.
#[derive(Debug, Clone)]
pub(crate) struct SipAddressInfo {
	/// The BIP32 derivation index.
	pub index: u32,
	/// The derived user public key.
	pub user_pubkey: PublicKey,
	/// The generated on-chain address.
	pub address: Address,
}

/// Manages swap-in-potentiam addresses and tracks UTXOs deposited to them.
///
/// Key derivation uses BIP32: user keys are derived at `m/787'/<index>` from the node's
/// master key. The server (LSP) public key and CSV delay are obtained during the initial
/// SIP protocol exchange and remain fixed for the lifetime of the wallet.
pub(crate) struct SipWallet {
	/// BIP32 extended private key for deriving user keys.
	user_xpriv: Xpriv,
	/// The server (LSP) public key used in all SIP addresses.
	server_pubkey: PublicKey,
	/// The CSV delay (in blocks) for the refund path.
	csv_delay: u16,
	/// Network (mainnet, testnet, regtest, etc.).
	network: Network,
	/// Generated SIP addresses indexed by their derivation index.
	addresses: Mutex<HashMap<u32, SipAddressInfo>>,
	/// Tracked SIP UTXOs keyed by outpoint.
	utxos: Mutex<HashMap<OutPoint, SipUtxo>>,
	/// Next unused derivation index.
	next_index: AtomicU32,
	logger: Arc<Logger>,
}

impl SipWallet {
	/// Creates a new SIP wallet.
	///
	/// The `master_xpriv` should be the node's master extended private key. SIP user keys are
	/// derived from it at `m/787'/<index>`.
	pub(crate) fn new(
		master_xpriv: Xpriv, server_pubkey: PublicKey, csv_delay: u16, network: Network,
		logger: Arc<Logger>,
	) -> Self {
		let secp = Secp256k1::new();
		let sip_xpriv = master_xpriv
			.derive_priv(
				&secp,
				&[ChildNumber::from_hardened_idx(SIP_HARDENED_CHILD_INDEX).unwrap()],
			)
			.expect("valid derivation");

		Self {
			user_xpriv: sip_xpriv,
			server_pubkey,
			csv_delay,
			network,
			addresses: Mutex::new(HashMap::new()),
			utxos: Mutex::new(HashMap::new()),
			next_index: AtomicU32::new(0),
			logger,
		}
	}

	/// Derives the user secret key for the given address index.
	fn derive_user_secret_key(&self, index: u32) -> SecretKey {
		let secp = Secp256k1::new();
		let child = self
			.user_xpriv
			.derive_priv(&secp, &[ChildNumber::from_normal_idx(index).unwrap()])
			.expect("valid derivation");
		child.private_key
	}

	/// Derives the user public key for the given address index.
	pub(crate) fn derive_user_pubkey(&self, index: u32) -> PublicKey {
		let secp = Secp256k1::new();
		PublicKey::from_secret_key(&secp, &self.derive_user_secret_key(index))
	}

	/// Returns the server (LSP) public key.
	pub(crate) fn server_pubkey(&self) -> PublicKey {
		self.server_pubkey
	}

	/// Returns the CSV delay in blocks.
	pub(crate) fn csv_delay(&self) -> u16 {
		self.csv_delay
	}

	/// Generates a new SIP deposit address.
	///
	/// Each call increments the internal derivation index, producing a unique address.
	pub(crate) fn new_address(&self) -> SipAddressInfo {
		let index = self.next_index.fetch_add(1, Ordering::Relaxed);
		let user_pk = self.derive_user_pubkey(index);
		let witness_script =
			build_sip_witness_script(&user_pk, &self.server_pubkey, self.csv_delay);
		let address = sip_p2wsh_address(&witness_script, self.network);

		let info = SipAddressInfo { index, user_pubkey: user_pk, address: address.clone() };

		self.addresses.lock().unwrap().insert(index, info.clone());

		log_info!(self.logger, "Generated SIP address {} (index {})", address, index);
		info
	}

	/// Returns the satisfaction weight for cooperative spends from SIP addresses managed by this
	/// wallet. This is constant for all addresses since they share the same script structure.
	pub(crate) fn cooperative_satisfaction_weight(&self) -> bitcoin::Weight {
		// Use index 0 as representative -- all SIP addresses have the same script structure
		// and thus the same satisfaction weight.
		let user_pk = self.derive_user_pubkey(0);
		let witness_script =
			build_sip_witness_script(&user_pk, &self.server_pubkey, self.csv_delay);
		cooperative_spend_satisfaction_weight(&witness_script)
	}

	/// Returns all script pubkeys that should be monitored for deposits.
	///
	/// The chain source should watch these for incoming transactions. This returns the
	/// P2WSH scriptPubKey for each generated SIP address.
	pub(crate) fn script_pubkeys_to_watch(&self) -> Vec<bitcoin::ScriptBuf> {
		let addresses = self.addresses.lock().unwrap();
		addresses
			.values()
			.map(|info| {
				let witness_script = build_sip_witness_script(
					&info.user_pubkey,
					&self.server_pubkey,
					self.csv_delay,
				);
				witness_script.to_p2wsh()
			})
			.collect()
	}

	/// Registers a newly discovered UTXO at a SIP address.
	///
	/// Called when the chain source discovers a deposit to one of our SIP addresses.
	pub(crate) fn register_utxo(
		&self, outpoint: OutPoint, value: Amount, address_index: u32, prevtx: Transaction,
	) {
		let user_pk = self.derive_user_pubkey(address_index);

		let utxo = SipUtxo {
			outpoint,
			value,
			address_index,
			user_pubkey: user_pk,
			server_pubkey: self.server_pubkey,
			csv_delay: self.csv_delay,
			state: SipUtxoState::Unconfirmed,
			prevtx,
		};

		log_debug!(
			self.logger,
			"Registered SIP UTXO {} with value {} (address index {})",
			outpoint,
			value,
			address_index
		);

		self.utxos.lock().unwrap().insert(outpoint, utxo);
	}

	/// Updates a UTXO's state to confirmed.
	pub(crate) fn confirm_utxo(&self, outpoint: &OutPoint, confirmed_at_height: u32) {
		let mut utxos = self.utxos.lock().unwrap();
		if let Some(utxo) = utxos.get_mut(outpoint) {
			if matches!(utxo.state, SipUtxoState::Unconfirmed) {
				log_info!(
					self.logger,
					"SIP UTXO {} confirmed at height {}",
					outpoint,
					confirmed_at_height
				);
				utxo.state = SipUtxoState::Confirmed { confirmed_at_height };
			}
		}
	}

	/// Updates UTXO states based on the current chain tip height.
	///
	/// Transitions confirmed UTXOs to `CsvExpired` when the relative timelock has elapsed.
	pub(crate) fn update_csv_expiry(&self, current_height: u32) {
		let mut utxos = self.utxos.lock().unwrap();
		for utxo in utxos.values_mut() {
			if utxo.csv_expired(current_height)
				&& matches!(utxo.state, SipUtxoState::Confirmed { .. })
			{
				log_info!(
					self.logger,
					"SIP UTXO {} CSV has expired at height {}",
					utxo.outpoint,
					current_height
				);
				utxo.state = SipUtxoState::CsvExpired;
			}
		}
	}

	/// Returns all tracked SIP UTXOs that are confirmed and eligible for swapping.
	pub(crate) fn swappable_utxos(&self) -> Vec<SipUtxo> {
		self.utxos.lock().unwrap().values().filter(|u| u.is_swappable()).cloned().collect()
	}

	/// Returns all tracked SIP UTXOs whose CSV has expired and are eligible for refund.
	pub(crate) fn refundable_utxos(&self) -> Vec<SipUtxo> {
		self.utxos.lock().unwrap().values().filter(|u| u.is_refundable()).cloned().collect()
	}

	/// Returns information about all tracked SIP UTXOs for the public API.
	pub(crate) fn list_utxos(&self) -> Vec<SipUtxoInfo> {
		self.utxos.lock().unwrap().values().map(SipUtxoInfo::from).collect()
	}

	/// Returns the total balance across all non-terminal SIP UTXOs.
	pub(crate) fn total_balance(&self) -> Amount {
		self.utxos.lock().unwrap().values().filter(|u| !u.is_terminal()).map(|u| u.value).sum()
	}

	/// Returns the balance of confirmed, swappable SIP UTXOs.
	pub(crate) fn spendable_balance(&self) -> Amount {
		self.utxos.lock().unwrap().values().filter(|u| u.is_swappable()).map(|u| u.value).sum()
	}

	/// Returns the balance of unconfirmed SIP UTXOs.
	pub(crate) fn pending_balance(&self) -> Amount {
		self.utxos
			.lock()
			.unwrap()
			.values()
			.filter(|u| matches!(u.state, SipUtxoState::Unconfirmed))
			.map(|u| u.value)
			.sum()
	}

	/// Returns the user secret key for signing a cooperative or refund spend.
	///
	/// This is used by the SIP protocol handlers when constructing spending transactions.
	pub(crate) fn signing_key(&self, address_index: u32) -> SecretKey {
		self.derive_user_secret_key(address_index)
	}

	/// Looks up the address index for a given SIP address, if it was generated by this wallet.
	pub(crate) fn address_index_for(&self, address: &Address) -> Option<u32> {
		let addresses = self.addresses.lock().unwrap();
		addresses.iter().find(|(_, info)| &info.address == address).map(|(index, _)| *index)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn test_wallet() -> SipWallet {
		let secp = Secp256k1::new();
		let master_xpriv =
			Xpriv::new_master(Network::Regtest, &[0x42; 32]).expect("valid master key");

		let server_sk = SecretKey::from_slice(&[0x22; 32]).unwrap();
		let server_pk = PublicKey::from_secret_key(&secp, &server_sk);

		SipWallet::new(
			master_xpriv,
			server_pk,
			2016,
			Network::Regtest,
			Arc::new(Logger::new_log_facade()),
		)
	}

	#[test]
	fn test_address_generation() {
		let wallet = test_wallet();
		let addr1 = wallet.new_address();
		let addr2 = wallet.new_address();

		assert_eq!(addr1.index, 0);
		assert_eq!(addr2.index, 1);
		assert_ne!(addr1.address, addr2.address);
		assert_ne!(addr1.user_pubkey, addr2.user_pubkey);
	}

	#[test]
	fn test_deterministic_derivation() {
		let wallet = test_wallet();
		let pk_a = wallet.derive_user_pubkey(0);
		let pk_b = wallet.derive_user_pubkey(0);
		assert_eq!(pk_a, pk_b);
	}

	#[test]
	fn test_utxo_lifecycle() {
		let wallet = test_wallet();
		let addr = wallet.new_address();

		let dummy_tx = Transaction {
			version: bitcoin::transaction::Version::TWO,
			lock_time: bitcoin::absolute::LockTime::ZERO,
			input: vec![],
			output: vec![bitcoin::TxOut {
				value: Amount::from_sat(50_000),
				script_pubkey: bitcoin::ScriptBuf::new(),
			}],
		};
		let outpoint = OutPoint::new(dummy_tx.compute_txid(), 0);

		// Register and check balances.
		wallet.register_utxo(outpoint, Amount::from_sat(50_000), addr.index, dummy_tx);
		assert_eq!(wallet.total_balance(), Amount::from_sat(50_000));
		assert_eq!(wallet.pending_balance(), Amount::from_sat(50_000));
		assert_eq!(wallet.spendable_balance(), Amount::ZERO);
		assert!(wallet.swappable_utxos().is_empty());

		// Confirm.
		wallet.confirm_utxo(&outpoint, 800_000);
		assert_eq!(wallet.spendable_balance(), Amount::from_sat(50_000));
		assert_eq!(wallet.pending_balance(), Amount::ZERO);
		assert_eq!(wallet.swappable_utxos().len(), 1);

		// CSV not yet expired.
		wallet.update_csv_expiry(800_100);
		assert!(wallet.refundable_utxos().is_empty());

		// CSV expired.
		wallet.update_csv_expiry(802_016);
		assert_eq!(wallet.refundable_utxos().len(), 1);
		assert!(wallet.swappable_utxos().is_empty());
	}

	#[test]
	fn test_script_pubkeys_to_watch() {
		let wallet = test_wallet();
		let _addr1 = wallet.new_address();
		let _addr2 = wallet.new_address();

		let spks = wallet.script_pubkeys_to_watch();
		assert_eq!(spks.len(), 2);
		// All should be P2WSH (starts with OP_0 + 32-byte push).
		for spk in &spks {
			assert!(spk.is_p2wsh());
		}
	}

	#[test]
	fn test_address_index_lookup() {
		let wallet = test_wallet();
		let addr = wallet.new_address();

		assert_eq!(wallet.address_index_for(&addr.address), Some(0));
	}
}

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
use bitcoin::{Address, Amount, FeeRate, Network, OutPoint, Transaction, Txid};

use lightning_liquidity::sip::address::{
	build_refund_witness, build_sip_witness_script, cooperative_spend_satisfaction_weight,
	refund_spend_satisfaction_weight, sip_p2wsh_address,
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
pub struct SipAddressInfo {
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
pub struct SipWallet {
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
	pub fn new(
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
	pub fn derive_user_pubkey(&self, index: u32) -> PublicKey {
		let secp = Secp256k1::new();
		PublicKey::from_secret_key(&secp, &self.derive_user_secret_key(index))
	}

	/// Returns the server (LSP) public key.
	pub fn server_pubkey(&self) -> PublicKey {
		self.server_pubkey
	}

	/// Returns the CSV delay in blocks.
	pub fn csv_delay(&self) -> u16 {
		self.csv_delay
	}

	/// Generates a new SIP deposit address.
	///
	/// Each call increments the internal derivation index, producing a unique address.
	pub fn new_address(&self) -> SipAddressInfo {
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
	pub fn cooperative_satisfaction_weight(&self) -> bitcoin::Weight {
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
	pub fn script_pubkeys_to_watch(&self) -> Vec<bitcoin::ScriptBuf> {
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
	pub fn register_utxo(
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
	pub fn confirm_utxo(&self, outpoint: &OutPoint, confirmed_at_height: u32) {
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
	pub fn update_csv_expiry(&self, current_height: u32) {
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
	pub fn swappable_utxos(&self) -> Vec<SipUtxo> {
		self.utxos.lock().unwrap().values().filter(|u| u.is_swappable()).cloned().collect()
	}

	/// Returns all tracked SIP UTXOs whose CSV has expired and are eligible for refund.
	pub fn refundable_utxos(&self) -> Vec<SipUtxo> {
		self.utxos.lock().unwrap().values().filter(|u| u.is_refundable()).cloned().collect()
	}

	/// Returns information about all tracked SIP UTXOs for the public API.
	pub fn list_utxos(&self) -> Vec<SipUtxoInfo> {
		self.utxos.lock().unwrap().values().map(SipUtxoInfo::from).collect()
	}

	/// Returns the total balance across all non-terminal SIP UTXOs.
	pub fn total_balance(&self) -> Amount {
		self.utxos.lock().unwrap().values().filter(|u| !u.is_terminal()).map(|u| u.value).sum()
	}

	/// Returns the balance of confirmed, swappable SIP UTXOs.
	pub fn spendable_balance(&self) -> Amount {
		self.utxos.lock().unwrap().values().filter(|u| u.is_swappable()).map(|u| u.value).sum()
	}

	/// Returns the balance of unconfirmed SIP UTXOs.
	pub fn pending_balance(&self) -> Amount {
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
	pub fn signing_key(&self, address_index: u32) -> SecretKey {
		self.derive_user_secret_key(address_index)
	}

	/// Looks up the address index for a given SIP address, if it was generated by this wallet.
	pub fn address_index_for(&self, address: &Address) -> Option<u32> {
		let addresses = self.addresses.lock().unwrap();
		addresses.iter().find(|(_, info)| &info.address == address).map(|(index, _)| *index)
	}

	/// Marks a UTXO as swap-initiated.
	pub fn mark_swap_initiated(
		&self, outpoint: &OutPoint, channel_id: lightning::ln::types::ChannelId,
	) {
		let mut utxos = self.utxos.lock().unwrap();
		if let Some(utxo) = utxos.get_mut(outpoint) {
			if utxo.is_swappable() {
				log_info!(
					self.logger,
					"SIP UTXO {} swap initiated for channel {}",
					outpoint,
					channel_id
				);
				utxo.state =
					SipUtxoState::SwapInitiated { channel_id };
			}
		}
	}

	/// Marks a UTXO as swapped (terminal state).
	pub fn mark_swapped(
		&self, outpoint: &OutPoint, channel_id: lightning::ln::types::ChannelId,
	) {
		let mut utxos = self.utxos.lock().unwrap();
		if let Some(utxo) = utxos.get_mut(outpoint) {
			log_info!(self.logger, "SIP UTXO {} swap completed for channel {}", outpoint, channel_id);
			utxo.state = SipUtxoState::Swapped { channel_id };
		}
	}

	/// Marks a UTXO as refunded (terminal state).
	pub fn mark_refunded(&self, outpoint: &OutPoint, spending_txid: Txid) {
		let mut utxos = self.utxos.lock().unwrap();
		if let Some(utxo) = utxos.get_mut(outpoint) {
			log_info!(self.logger, "SIP UTXO {} refunded via {}", outpoint, spending_txid);
			utxo.state = SipUtxoState::Refunded { spending_txid };
		}
	}

	/// Builds a refund transaction sweeping all expired SIP UTXOs to the given destination.
	///
	/// Returns the signed transaction and the outpoints being swept, or `None` if no UTXOs are
	/// eligible for refund.
	pub fn build_refund_transaction(
		&self, destination: bitcoin::ScriptBuf, fee_rate: FeeRate,
	) -> Option<(Transaction, Vec<OutPoint>)> {
		let secp = Secp256k1::new();
		let refundable = self.refundable_utxos();
		if refundable.is_empty() {
			return None;
		}

		let mut inputs = Vec::new();
		let mut outpoints = Vec::new();
		let mut total_value = Amount::ZERO;

		for utxo in &refundable {
			let witness_script = build_sip_witness_script(
				&utxo.user_pubkey,
				&utxo.server_pubkey,
				utxo.csv_delay,
			);

			inputs.push(bitcoin::TxIn {
				previous_output: utxo.outpoint,
				script_sig: bitcoin::ScriptBuf::new(),
				sequence: bitcoin::Sequence::from_consensus(utxo.csv_delay as u32),
				witness: bitcoin::Witness::new(),
			});
			outpoints.push(utxo.outpoint);
			total_value += utxo.value;
		}

		// Estimate fee.
		let first_utxo = &refundable[0];
		let witness_script = build_sip_witness_script(
			&first_utxo.user_pubkey,
			&first_utxo.server_pubkey,
			first_utxo.csv_delay,
		);
		let input_weight = refund_spend_satisfaction_weight(&witness_script);
		// Base tx weight (version + locktime + input/output counts) + per-input + one output.
		let estimated_weight = bitcoin::Weight::from_wu(40 * 4) // base fields
			+ input_weight * inputs.len() as u64
			+ bitcoin::Weight::from_wu(43 * 4); // P2WPKH output estimate
		let fee = fee_rate * estimated_weight;

		let output_value = total_value.checked_sub(fee)?;
		if output_value <= Amount::from_sat(546) {
			// Dust output, not worth sweeping.
			log_info!(
				self.logger,
				"SIP refund would produce dust output ({} after {} fee), skipping",
				output_value,
				fee
			);
			return None;
		}

		let mut tx = Transaction {
			version: bitcoin::transaction::Version::TWO,
			lock_time: bitcoin::absolute::LockTime::ZERO,
			input: inputs,
			output: vec![bitcoin::TxOut { value: output_value, script_pubkey: destination }],
		};

		// Sign each input with the refund path.
		for (i, utxo) in refundable.iter().enumerate() {
			let witness_script = build_sip_witness_script(
				&utxo.user_pubkey,
				&utxo.server_pubkey,
				utxo.csv_delay,
			);

			let sighash = bitcoin::sighash::SighashCache::new(&tx)
				.p2wsh_signature_hash(
					i,
					&witness_script,
					utxo.value,
					bitcoin::EcdsaSighashType::All,
				)
				.expect("valid sighash");

			let msg = bitcoin::secp256k1::Message::from_digest(
				bitcoin::hashes::Hash::to_byte_array(sighash),
			);
			let sk = self.derive_user_secret_key(utxo.address_index);
			let sig = bitcoin::ecdsa::Signature {
				signature: secp.sign_ecdsa(&msg, &sk),
				sighash_type: bitcoin::EcdsaSighashType::All,
			};

			tx.input[i].witness = build_refund_witness(&sig, &witness_script);
		}

		Some((tx, outpoints))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use bitcoin::hashes::Hash;

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

	#[test]
	fn test_refund_transaction() {
		let wallet = test_wallet();
		let addr = wallet.new_address();

		// Create a proper prevtx with the SIP script_pubkey.
		let witness_script = build_sip_witness_script(
			&addr.user_pubkey,
			&wallet.server_pubkey(),
			wallet.csv_delay(),
		);
		let script_pubkey = witness_script.to_p2wsh();

		let prevtx = Transaction {
			version: bitcoin::transaction::Version::TWO,
			lock_time: bitcoin::absolute::LockTime::ZERO,
			input: vec![bitcoin::TxIn {
				previous_output: OutPoint::null(),
				script_sig: bitcoin::ScriptBuf::new(),
				sequence: bitcoin::Sequence::MAX,
				witness: bitcoin::Witness::new(),
			}],
			output: vec![bitcoin::TxOut { value: Amount::from_sat(100_000), script_pubkey }],
		};
		let outpoint = OutPoint::new(prevtx.compute_txid(), 0);

		wallet.register_utxo(outpoint, Amount::from_sat(100_000), addr.index, prevtx);
		wallet.confirm_utxo(&outpoint, 800_000);

		// Not yet expired.
		let dest = bitcoin::ScriptBuf::new_p2wpkh(
			&bitcoin::WPubkeyHash::from_slice(&[0; 20]).unwrap(),
		);
		assert!(wallet.build_refund_transaction(dest.clone(), FeeRate::from_sat_per_vb(2).unwrap()).is_none());

		// Expire the CSV.
		wallet.update_csv_expiry(802_016);

		let result = wallet.build_refund_transaction(dest, FeeRate::from_sat_per_vb(2).unwrap());
		assert!(result.is_some());

		let (tx, swept_outpoints) = result.unwrap();
		assert_eq!(swept_outpoints.len(), 1);
		assert_eq!(swept_outpoints[0], outpoint);
		assert_eq!(tx.input.len(), 1);
		assert_eq!(tx.output.len(), 1);
		// Output value should be less than input (fee deducted).
		assert!(tx.output[0].value < Amount::from_sat(100_000));
		// Input sequence should be the CSV delay.
		assert_eq!(tx.input[0].sequence, bitcoin::Sequence::from_consensus(2016));
		// Witness should be the refund path (3 items).
		assert_eq!(tx.input[0].witness.len(), 3);
	}
}

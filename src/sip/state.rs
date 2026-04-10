// This file is Copyright its original authors, visible in version control history.
//
// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. You may not use this file except in
// accordance with one or both of these licenses.

//! UTXO state machine for swap-in-potentiam.

use bitcoin::secp256k1::PublicKey;
use bitcoin::{Amount, OutPoint, Transaction, Txid};

use lightning::ln::types::ChannelId;

/// The lifecycle state of a swap-in-potentiam UTXO.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SipUtxoState {
	/// The deposit has been seen in the mempool but is not yet confirmed.
	Unconfirmed,
	/// The deposit is confirmed on-chain. The UTXO is eligible for swapping once the LSP
	/// considers it sufficiently confirmed (but well before the CSV expiry).
	Confirmed {
		/// The block height at which the UTXO was first confirmed.
		confirmed_at_height: u32,
	},
	/// A swap into a Lightning channel has been initiated using this UTXO. The channel
	/// funding or splice transaction has been constructed but may not yet be confirmed.
	SwapInitiated {
		/// The channel this UTXO is being swapped into.
		channel_id: ChannelId,
	},
	/// The swap is complete: the channel funding or splice transaction has confirmed.
	Swapped {
		/// The channel this UTXO was swapped into.
		channel_id: ChannelId,
	},
	/// The CSV relative timelock has expired. The user can now unilaterally reclaim the funds
	/// via the refund spending path without the server's cooperation.
	CsvExpired,
	/// The user has reclaimed the UTXO via the refund spending path.
	Refunded {
		/// The txid of the refund transaction that spent this UTXO.
		spending_txid: Txid,
	},
}

/// Information about a single swap-in-potentiam UTXO tracked by the wallet.
#[derive(Debug, Clone)]
pub struct SipUtxo {
	/// The outpoint identifying this UTXO on-chain.
	pub outpoint: OutPoint,
	/// The value of this UTXO.
	pub value: Amount,
	/// The BIP32 derivation index used to derive the user key for this UTXO's SIP address.
	pub address_index: u32,
	/// The user's public key for this specific SIP address.
	pub user_pubkey: PublicKey,
	/// The server (LSP) public key used in this SIP address.
	pub server_pubkey: PublicKey,
	/// The CSV delay (in blocks) for the refund path.
	pub csv_delay: u16,
	/// The current state of this UTXO.
	pub state: SipUtxoState,
	/// The full previous transaction containing this UTXO. Needed for constructing the spending
	/// witness (required for P2WSH inputs in the interactive tx protocol).
	pub prevtx: Transaction,
}

impl SipUtxo {
	/// Returns `true` if this UTXO is confirmed and eligible for swapping (not yet swapped or
	/// expired).
	pub fn is_swappable(&self) -> bool {
		matches!(self.state, SipUtxoState::Confirmed { .. })
	}

	/// Returns `true` if the CSV timelock has expired and the user can unilaterally reclaim
	/// the funds.
	pub fn is_refundable(&self) -> bool {
		matches!(self.state, SipUtxoState::CsvExpired)
	}

	/// Returns `true` if this UTXO is in a terminal state (swapped or refunded).
	pub fn is_terminal(&self) -> bool {
		matches!(self.state, SipUtxoState::Swapped { .. } | SipUtxoState::Refunded { .. })
	}

	/// Returns the number of confirmations given the current chain tip, or `None` if unconfirmed.
	pub fn confirmations(&self, current_height: u32) -> Option<u32> {
		match self.state {
			SipUtxoState::Confirmed { confirmed_at_height } => {
				Some(current_height.saturating_sub(confirmed_at_height) + 1)
			},
			_ => None,
		}
	}

	/// Checks whether the CSV timelock has expired given the current block height.
	///
	/// The CSV is relative to the confirmation height: the UTXO becomes refundable when
	/// `current_height >= confirmed_at_height + csv_delay`.
	pub fn csv_expired(&self, current_height: u32) -> bool {
		match self.state {
			SipUtxoState::Confirmed { confirmed_at_height } => {
				current_height >= confirmed_at_height + self.csv_delay as u32
			},
			SipUtxoState::CsvExpired => true,
			_ => false,
		}
	}
}

/// User-facing information about a SIP UTXO, returned by public API methods.
#[derive(Debug, Clone)]
pub struct SipUtxoInfo {
	/// The outpoint identifying this UTXO.
	pub outpoint: OutPoint,
	/// The value of this UTXO.
	pub value: Amount,
	/// The current state.
	pub state: SipUtxoState,
}

impl From<&SipUtxo> for SipUtxoInfo {
	fn from(utxo: &SipUtxo) -> Self {
		Self { outpoint: utxo.outpoint, value: utxo.value, state: utxo.state.clone() }
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	use bitcoin::secp256k1::{Secp256k1, SecretKey};
	use bitcoin::{transaction, Amount, Sequence, TxIn, TxOut, Witness};

	fn test_utxo() -> SipUtxo {
		let secp = Secp256k1::new();
		let user_sk = SecretKey::from_slice(&[0x11; 32]).unwrap();
		let user_pk = PublicKey::from_secret_key(&secp, &user_sk);
		let server_sk = SecretKey::from_slice(&[0x22; 32]).unwrap();
		let server_pk = PublicKey::from_secret_key(&secp, &server_sk);

		let prevtx = Transaction {
			version: transaction::Version::TWO,
			lock_time: bitcoin::absolute::LockTime::ZERO,
			input: vec![TxIn {
				previous_output: OutPoint::null(),
				script_sig: bitcoin::ScriptBuf::new(),
				sequence: Sequence::MAX,
				witness: Witness::new(),
			}],
			output: vec![TxOut {
				value: Amount::from_sat(100_000),
				script_pubkey: bitcoin::ScriptBuf::new(),
			}],
		};

		SipUtxo {
			outpoint: OutPoint::new(prevtx.compute_txid(), 0),
			value: Amount::from_sat(100_000),
			address_index: 0,
			user_pubkey: user_pk,
			server_pubkey: server_pk,
			csv_delay: 2016,
			state: SipUtxoState::Unconfirmed,
			prevtx,
		}
	}

	#[test]
	fn test_state_transitions() {
		let mut utxo = test_utxo();

		assert!(!utxo.is_swappable());
		assert!(!utxo.is_refundable());
		assert!(!utxo.is_terminal());

		utxo.state = SipUtxoState::Confirmed { confirmed_at_height: 800_000 };
		assert!(utxo.is_swappable());
		assert!(!utxo.is_refundable());
		assert_eq!(utxo.confirmations(800_005), Some(6));

		let channel_id = ChannelId::new_zero();
		utxo.state = SipUtxoState::SwapInitiated { channel_id };
		assert!(!utxo.is_swappable());

		utxo.state = SipUtxoState::Swapped { channel_id };
		assert!(utxo.is_terminal());
	}

	#[test]
	fn test_csv_expiry() {
		let mut utxo = test_utxo();
		utxo.csv_delay = 100;
		utxo.state = SipUtxoState::Confirmed { confirmed_at_height: 800_000 };

		assert!(!utxo.csv_expired(800_050));
		assert!(!utxo.csv_expired(800_099));
		assert!(utxo.csv_expired(800_100));
		assert!(utxo.csv_expired(900_000));
	}
}

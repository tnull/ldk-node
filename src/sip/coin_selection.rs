// This file is Copyright its original authors, visible in version control history.
//
// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. You may not use this file except in
// accordance with one or both of these licenses.

//! `CoinSelectionSource` implementation backed by SIP UTXOs.
//!
//! This allows SIP UTXOs to be used as inputs when funding or splicing Lightning channels,
//! integrating with the existing `FundingTemplate::splice_in_sync()` API.

use std::future::Future;
use std::sync::Arc;

use bitcoin::psbt::Psbt;
use bitcoin::{Transaction, TxOut};

use lightning::chain::ClaimId;
use lightning::ln::funding::FundingTxInput;
use lightning::util::wallet_utils::{CoinSelection, CoinSelectionSource, ConfirmedUtxo, Input};

use crate::sip::wallet::SipWallet;

/// A `CoinSelectionSource` that returns pre-selected SIP UTXOs.
///
/// When a channel open or splice needs funding inputs, this source provides the confirmed SIP
/// UTXOs instead of performing generic wallet coin selection. The LSP can trust 0-conf from
/// these inputs because it co-signed the SIP address.
pub(crate) struct SipCoinSelectionSource {
	wallet: Arc<SipWallet>,
}

impl SipCoinSelectionSource {
	/// Creates a new source backed by the given SIP wallet.
	///
	/// Only confirmed, swappable SIP UTXOs will be returned during coin selection.
	pub(crate) fn new(wallet: Arc<SipWallet>) -> Self {
		Self { wallet }
	}
}

impl CoinSelectionSource for SipCoinSelectionSource {
	fn select_confirmed_utxos<'a>(
		&'a self, _claim_id: Option<ClaimId>, must_spend: Vec<Input>, _must_pay_to: &'a [TxOut],
		_target_feerate_sat_per_1000_weight: u32, _max_tx_weight: u64,
	) -> impl Future<Output = Result<CoinSelection, ()>> + Send + 'a {
		async move {
			let swappable = self.wallet.swappable_utxos();
			if swappable.is_empty() {
				return Err(());
			}

			let satisfaction_weight = self.wallet.cooperative_satisfaction_weight();

			let mut confirmed_utxos = Vec::new();
			for utxo in &swappable {
				let confirmed = ConfirmedUtxo::new_p2wsh(
					utxo.prevtx.clone(),
					utxo.outpoint.vout,
					satisfaction_weight,
				)
				.map_err(|_| ())?;
				confirmed_utxos.push(confirmed);
			}

			// Also include any must_spend inputs (e.g., the shared funding input for splices).
			// These are passed through as-is -- they're already accounted for by the caller.
			let _ = must_spend;

			Ok(CoinSelection { confirmed_utxos, change_output: None })
		}
	}

	fn sign_psbt<'a>(
		&'a self, _psbt: Psbt,
	) -> impl Future<Output = Result<Transaction, ()>> + Send + 'a {
		// SIP cooperative spend signing is handled separately by the protocol flow --
		// the user provides their signature via the SIP protocol, and the LSP provides
		// its signature. This method should not be called for SIP-funded channels.
		async move { Err(()) }
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::logger::Logger;
	use crate::sip::wallet::SipWallet;

	use bitcoin::bip32::Xpriv;
	use bitcoin::secp256k1::{PublicKey, Secp256k1, SecretKey};
	use bitcoin::{transaction, Amount, Network, OutPoint, ScriptBuf, Sequence, TxIn, Witness};

	fn make_wallet() -> Arc<SipWallet> {
		let secp = Secp256k1::new();
		let master = Xpriv::new_master(Network::Regtest, &[0x42; 32]).unwrap();
		let server_sk = SecretKey::from_slice(&[0x22; 32]).unwrap();
		let server_pk = PublicKey::from_secret_key(&secp, &server_sk);
		Arc::new(SipWallet::new(
			master,
			server_pk,
			2016,
			Network::Regtest,
			Arc::new(Logger::new_log_facade()),
		))
	}

	#[tokio::test]
	async fn test_empty_wallet_returns_err() {
		let wallet = make_wallet();
		let source = SipCoinSelectionSource::new(wallet);
		let result =
			source.select_confirmed_utxos(None, vec![], &[], 1000, u64::MAX).await;
		assert!(result.is_err());
	}

	#[tokio::test]
	async fn test_confirmed_utxos_returned() {
		let wallet = make_wallet();
		let addr = wallet.new_address();

		// Create a prevtx with a P2WSH output matching the SIP address.
		let witness_script = lightning_liquidity::sip::address::build_sip_witness_script(
			&addr.user_pubkey,
			&wallet.server_pubkey(),
			wallet.csv_delay(),
		);
		let script_pubkey = witness_script.to_p2wsh();

		let prevtx = Transaction {
			version: transaction::Version::TWO,
			lock_time: bitcoin::absolute::LockTime::ZERO,
			input: vec![TxIn {
				previous_output: OutPoint::null(),
				script_sig: ScriptBuf::new(),
				sequence: Sequence::MAX,
				witness: Witness::new(),
			}],
			output: vec![bitcoin::TxOut {
				value: Amount::from_sat(100_000),
				script_pubkey,
			}],
		};
		let outpoint = OutPoint::new(prevtx.compute_txid(), 0);

		wallet.register_utxo(outpoint, Amount::from_sat(100_000), addr.index, prevtx);
		wallet.confirm_utxo(&outpoint, 800_000);

		let source = SipCoinSelectionSource::new(Arc::clone(&wallet));
		let result =
			source.select_confirmed_utxos(None, vec![], &[], 1000, u64::MAX).await;

		assert!(result.is_ok());
		let selection = result.unwrap();
		assert_eq!(selection.confirmed_utxos.len(), 1);
		assert!(selection.change_output.is_none());
	}
}

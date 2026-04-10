// This file is Copyright its original authors, visible in version control history.
//
// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. You may not use this file except in
// accordance with one or both of these licenses.

//! Swap-in-potentiam wallet, state management, and channel integration.
//!
//! This module provides a BDK-integrated wallet for managing swap-in-potentiam (SIP) addresses
//! and UTXOs. It leverages the address construction primitives from `lightning_liquidity::sip` and
//! adds wallet-level functionality: key derivation, UTXO discovery via chain sync, state tracking,
//! and transaction building for refund and cooperative spends.

pub(crate) mod coin_selection;
/// UTXO state machine for swap-in-potentiam.
pub mod state;
/// SIP wallet with BIP32 key derivation and UTXO tracking.
pub mod wallet;

use std::sync::Arc;

use bitcoin::bip32::Xpriv;
use bitcoin::secp256k1::PublicKey;
use bitcoin::Network;

use crate::logger::{log_error, log_info, LdkLogger, Logger};
use crate::sip::state::SipUtxoInfo;
use crate::sip::wallet::{SipAddressInfo, SipWallet};

/// Orchestrates the SIP wallet and protocol layers.
///
/// The `SipManager` bridges the SIP wallet (UTXO tracking, key derivation, chain monitoring)
/// with the SIP protocol handlers in `lightning-liquidity` (message exchange with the LSP) and
/// with ldk-node's channel management (funding channels and splicing from SIP UTXOs).
pub(crate) struct SipManager {
	wallet: Arc<SipWallet>,
	logger: Arc<Logger>,
}

impl SipManager {
	/// Creates a new `SipManager`.
	///
	/// The `master_xpriv` should be the node's master extended private key. The `server_pubkey`
	/// and `csv_delay` are obtained from the LSP during the `sip.get_info` exchange.
	pub(crate) fn new(
		master_xpriv: Xpriv, server_pubkey: PublicKey, csv_delay: u16, network: Network,
		logger: Arc<Logger>,
	) -> Self {
		let wallet =
			Arc::new(SipWallet::new(master_xpriv, server_pubkey, csv_delay, network, logger.clone()));
		Self { wallet, logger }
	}

	/// Returns a reference to the SIP wallet.
	pub(crate) fn wallet(&self) -> &SipWallet {
		&self.wallet
	}

	/// Generates a new SIP deposit address.
	pub(crate) fn new_address(&self) -> SipAddressInfo {
		self.wallet.new_address()
	}

	/// Returns all tracked SIP UTXOs for the public API.
	pub(crate) fn list_utxos(&self) -> Vec<SipUtxoInfo> {
		self.wallet.list_utxos()
	}

	/// Updates UTXO states based on current chain tip.
	pub(crate) fn update_on_new_block(&self, current_height: u32) {
		self.wallet.update_csv_expiry(current_height);
	}

	/// Returns the SIP wallet for use as a `CoinSelectionSource` wrapper.
	pub(crate) fn wallet_arc(&self) -> Arc<SipWallet> {
		Arc::clone(&self.wallet)
	}
}

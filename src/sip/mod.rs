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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bitcoin::bip32::Xpriv;
use bitcoin::secp256k1::PublicKey;
use bitcoin::{Network, OutPoint, Transaction, Txid};
use lightning::ln::types::ChannelId;

use crate::logger::{log_error, log_info, LdkLogger, Logger};
use crate::sip::state::SipUtxoInfo;
use crate::sip::wallet::{SipAddressInfo, SipWallet};

/// A pending SIP funding transaction awaiting the server's cooperative signatures.
#[derive(Clone)]
pub(crate) struct PendingSipFunding {
	pub channel_id: ChannelId,
	pub counterparty_node_id: PublicKey,
	pub tx: Transaction,
	/// The SIP input indices in the transaction and their corresponding outpoints.
	pub sip_inputs: Vec<(usize, OutPoint)>,
	/// Whether this is a V1 channel open (uses `funding_transaction_generated`).
	pub is_v1_open: bool,
}

/// Tracks channel opens that should be funded from SIP UTXOs.
/// When `FundingGenerationReady` fires for one of these channels, the handler constructs
/// the funding tx from SIP UTXOs instead of the regular wallet.
pub(crate) struct PendingSipChannelOpen {
	pub user_channel_id: u128,
}

/// Orchestrates the SIP wallet and protocol layers.
///
/// The `SipManager` bridges the SIP wallet (UTXO tracking, key derivation, chain monitoring)
/// with the SIP protocol handlers in `lightning-liquidity` (message exchange with the LSP) and
/// with ldk-node's channel management (funding channels and splicing from SIP UTXOs).
pub(crate) struct SipManager {
	wallet: Arc<SipWallet>,
	/// Funding transactions waiting for the server's cooperative SIP signatures.
	pending_fundings: Mutex<HashMap<ChannelId, PendingSipFunding>>,
	/// Channel opens that should be funded from SIP UTXOs.
	pending_sip_opens: Mutex<Vec<u128>>,
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
		Self {
			wallet,
			pending_fundings: Mutex::new(HashMap::new()),
			pending_sip_opens: Mutex::new(Vec::new()),
			logger,
		}
	}

	/// Stashes a pending funding transaction that contains SIP inputs awaiting the server's
	/// cooperative signatures.
	pub(crate) fn stash_pending_funding(&self, pending: PendingSipFunding) {
		log_info!(
			self.logger,
			"Stashed SIP funding for channel {} with {} SIP inputs, awaiting server signatures",
			pending.channel_id,
			pending.sip_inputs.len(),
		);
		self.pending_fundings.lock().unwrap().insert(pending.channel_id, pending);
	}

	/// Takes the pending funding for the given channel, if any.
	pub(crate) fn take_pending_funding(
		&self, channel_id: &ChannelId,
	) -> Option<PendingSipFunding> {
		self.pending_fundings.lock().unwrap().remove(channel_id)
	}

	/// Registers a channel open as SIP-funded.
	pub(crate) fn register_sip_open(&self, user_channel_id: u128) {
		self.pending_sip_opens.lock().unwrap().push(user_channel_id);
	}

	/// Checks and removes a pending SIP open for the given user_channel_id.
	pub(crate) fn take_sip_open(&self, user_channel_id: u128) -> bool {
		let mut opens = self.pending_sip_opens.lock().unwrap();
		if let Some(pos) = opens.iter().position(|id| *id == user_channel_id) {
			opens.remove(pos);
			true
		} else {
			false
		}
	}

	/// Returns the channel IDs of pending SIP fundings awaiting server signatures.
	pub(crate) fn pending_funding_channel_ids(&self) -> Vec<ChannelId> {
		self.pending_fundings.lock().unwrap().keys().cloned().collect()
	}

	/// Returns a clone of the pending funding for the given channel, without removing it.
	pub(crate) fn peek_pending_funding(
		&self, channel_id: &ChannelId,
	) -> Option<PendingSipFunding> {
		self.pending_fundings.lock().unwrap().get(channel_id).cloned()
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

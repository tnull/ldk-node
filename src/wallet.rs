use crate::logger::{
	log_error, log_given_level, log_internal, log_trace, FilesystemLogger, Logger,
};
use crate::Error;

use lightning::chain::chaininterface::{
	BroadcasterInterface, ConfirmationTarget, FeeEstimator, FEERATE_FLOOR_SATS_PER_KW,
};

use bdk::blockchain::{Blockchain, EsploraBlockchain};
use bdk::database::BatchDatabase;
use bdk::wallet::AddressIndex;
use bdk::{SignOptions, SyncOptions};

use bitcoin::{Script, Transaction};

use std::sync::{Arc, Mutex};

pub struct Wallet<D>
where
	D: BatchDatabase,
{
	// A BDK blockchain used for wallet sync.
	blockchain: EsploraBlockchain,
	// A BDK on-chain wallet.
	wallet: Mutex<bdk::Wallet<D>>,
	logger: Arc<FilesystemLogger>,
}

impl<D> Wallet<D>
where
	D: BatchDatabase,
{
	pub(crate) fn new(
		blockchain: EsploraBlockchain, wallet: bdk::Wallet<D>, logger: Arc<FilesystemLogger>,
	) -> Self {
		let wallet = Mutex::new(wallet);
		Self {
			blockchain,
			wallet,
			logger,
		}
	}

	pub(crate) async fn sync(&self) -> Result<(), Error> {
		let sync_options = SyncOptions { progress: None };

		self.wallet.lock().unwrap().sync(&self.blockchain, sync_options)?;

		Ok(())
	}

	pub(crate) fn create_funding_transaction(
		&self, output_script: &Script, value_sats: u64, confirmation_target: ConfirmationTarget,
	) -> Result<Transaction, Error> {
		let num_blocks = num_blocks_from_conf_target(confirmation_target);
		let fee_rate = self.blockchain.estimate_fee(num_blocks)?;

		let locked_wallet = self.wallet.lock().unwrap();
		let mut tx_builder = locked_wallet.build_tx();

		tx_builder.add_recipient(output_script.clone(), value_sats).fee_rate(fee_rate).enable_rbf();

		let (mut psbt, _) = tx_builder.finish()?;
		log_trace!(self.logger, "Created funding PSBT: {:?}", psbt);

		// We double-check that no inputs try to spend non-witness outputs. As we use a SegWit
		// wallet descriptor this technically shouldn't ever happen, but better safe than sorry.
		for input in &psbt.inputs {
			if input.witness_utxo.is_none() {
				log_error!(self.logger, "Tried to spend a non-witness funding output. This must not ever happen. Panicking!");
				panic!("Tried to spend a non-witness funding output. This must not ever happen.");
			}
		}

		if !locked_wallet.sign(&mut psbt, SignOptions::default())? {
			return Err(Error::FundingTxCreationFailed);
		}

		Ok(psbt.extract_tx())
	}

	pub(crate) fn get_new_address(&self) -> Result<bitcoin::Address, Error> {
		let address_info = self.wallet.lock().unwrap().get_address(AddressIndex::New)?;
		Ok(address_info.address)
	}
}

impl<D> FeeEstimator for Wallet<D>
where
	D: BatchDatabase,
{
	fn get_est_sat_per_1000_weight(&self, confirmation_target: ConfirmationTarget) -> u32 {
		let num_blocks = num_blocks_from_conf_target(confirmation_target);
		let fallback_fee = fallback_fee_from_conf_target(confirmation_target);
		self.blockchain.estimate_fee(num_blocks).map_or(fallback_fee, |fee_rate| {
			(fee_rate.fee_wu(1000) as u32).max(FEERATE_FLOOR_SATS_PER_KW)
		}) as u32
	}
}

impl<D> BroadcasterInterface for Wallet<D>
where
	D: BatchDatabase,
{
	fn broadcast_transaction(&self, tx: &Transaction) {
		match self.blockchain.broadcast(tx) {
			Ok(_) => {}
			Err(err) => {
				log_error!(self.logger, "Failed to broadcast transaction: {}", err);
				panic!("Failed to broadcast transaction: {}", err);
			}
		}
	}
}

fn num_blocks_from_conf_target(confirmation_target: ConfirmationTarget) -> usize {
	match confirmation_target {
		ConfirmationTarget::Background => 12,
		ConfirmationTarget::Normal => 6,
		ConfirmationTarget::HighPriority => 3,
	}
}

fn fallback_fee_from_conf_target(confirmation_target: ConfirmationTarget) -> u32 {
	match confirmation_target {
		ConfirmationTarget::Background => FEERATE_FLOOR_SATS_PER_KW,
		ConfirmationTarget::Normal => 2000,
		ConfirmationTarget::HighPriority => 5000,
	}
}

use crate::hex_utils;
use crate::io::{
	SPENDABLE_OUTPUT_INFO_PERSISTENCE_PRIMARY_NAMESPACE,
	SPENDABLE_OUTPUT_INFO_PERSISTENCE_SECONDARY_NAMESPACE,
};
use crate::logger::{log_error, Logger};
use crate::wallet::{Wallet, WalletKeysManager};
use crate::Error;

use lightning::chain::chaininterface::{BroadcasterInterface, ConfirmationTarget, FeeEstimator};
use lightning::chain::{self, BestBlock, Confirm, Filter, Listen};
use lightning::sign::{EntropySource, SpendableOutputDescriptor};
use lightning::util::persist::KVStore;
use lightning::util::ser::Writeable;
use lightning::{impl_writeable_tlv_based, impl_writeable_tlv_based_enum};

use bitcoin::secp256k1::Secp256k1;
use bitcoin::{BlockHash, BlockHeader, LockTime, PackedLockTime, Transaction, Txid};

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, Mutex};

const CONSIDERED_SPENT_THRESHOLD_CONF: u32 = 6;

const REGENERATE_SPEND_THRESHOLD: u32 = 144;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BroadcastTx {
	tx: Transaction,
	broadcast_height: u32,
}

impl_writeable_tlv_based!(BroadcastTx, {
	(0, tx, required),
	(2, broadcast_height, required),
});

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ConfirmedTx {
	tx: Transaction,
	broadcast_height: u32,
	confirmation_height: u32,
	confirmation_hash: BlockHash,
}

impl_writeable_tlv_based!(ConfirmedTx, {
	(0, tx, required),
	(2, broadcast_height, required),
	(4, confirmation_height, required),
	(6, confirmation_hash, required),
});

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SpendableOutputStatus {
	Pending,
	Broadcast { txs: HashMap<Txid, BroadcastTx> },
	Confirmed { txs: HashMap<Txid, BroadcastTx>, confirmed_tx: ConfirmedTx },
}

impl_writeable_tlv_based_enum!(SpendableOutputStatus,
	(0, Pending) => {},
	(2, Broadcast) => {
		(0, txs, required),
	},
	(4, Confirmed) => {
		(0, txs, required),
		(2, confirmed_tx, required),
	};
);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SpendableOutputInfo {
	id: [u8; 32],
	descriptor: SpendableOutputDescriptor,
	status: SpendableOutputStatus,
}

impl SpendableOutputInfo {
	fn spending_txs(&self) -> Vec<&Transaction> {
		match self.status {
			SpendableOutputStatus::Pending { .. } => Vec::new(),
			SpendableOutputStatus::Broadcast { ref txs, .. } => {
				txs.iter().map(|(_, btx)| &btx.tx).collect()
			}
			SpendableOutputStatus::Confirmed { ref txs, ref confirmed_tx, .. } => {
				let mut res: Vec<&Transaction> = txs.iter().map(|(_, btx)| &btx.tx).collect();
				res.push(&confirmed_tx.tx);
				res
			}
		}
	}

	fn last_spending_tx(&self) -> Option<&Transaction> {
		match self.status {
			SpendableOutputStatus::Pending { .. } => None,
			SpendableOutputStatus::Broadcast { ref txs, .. } => {
				txs.iter().max_by_key(|(_, btx)| btx.broadcast_height).map(|(_, btx)| &btx.tx)
			}
			SpendableOutputStatus::Confirmed { ref confirmed_tx, .. } => Some(&confirmed_tx.tx),
		}
	}

	fn confirmed_tx(&self) -> Option<&ConfirmedTx> {
		match self.status {
			SpendableOutputStatus::Pending { .. } => None,
			SpendableOutputStatus::Broadcast { .. } => None,
			SpendableOutputStatus::Confirmed { ref confirmed_tx, .. } => Some(&confirmed_tx),
		}
	}

	fn tx_broadcast(&mut self, tx: Transaction, broadcast_height: u32) {
		match self.status {
			SpendableOutputStatus::Pending => {
				let mut txs = HashMap::new();
				let txid = tx.txid();
				let broadcast_tx = BroadcastTx { tx, broadcast_height };
				txs.insert(txid, broadcast_tx);
				self.status = SpendableOutputStatus::Broadcast { txs };
			}
			SpendableOutputStatus::Broadcast { ref mut txs } => {
				let txid = tx.txid();
				let broadcast_tx = BroadcastTx { tx, broadcast_height };
				txs.insert(txid, broadcast_tx);
			}
			SpendableOutputStatus::Confirmed { .. } => {
				debug_assert!(false, "We should never broadcast further transactions if we already saw a confirmation.");
			}
		}
	}

	fn tx_confirmed(&mut self, txid: Txid, confirmation_height: u32, confirmation_hash: BlockHash) {
		match self.status {
			SpendableOutputStatus::Pending => {
				debug_assert!(
					false,
					"We should never confirm a transaction if we haven't broadcast."
				);
			}
			SpendableOutputStatus::Broadcast { ref mut txs } => {
				if let Some(broadcast_tx) = txs.get_mut(&txid) {
					let confirmed_tx = ConfirmedTx {
						tx: broadcast_tx.tx.clone(),
						broadcast_height: broadcast_tx.broadcast_height,
						confirmation_height,
						confirmation_hash,
					};
					self.status =
						SpendableOutputStatus::Confirmed { txs: txs.clone(), confirmed_tx };
				} else {
					debug_assert!(
						false,
						"We should never confirm a transaction if we haven't broadcast."
					);
				}
			}
			SpendableOutputStatus::Confirmed { ref confirmed_tx, .. } => {
				if txid != confirmed_tx.tx.txid()
					|| confirmation_hash != confirmed_tx.confirmation_hash
				{
					debug_assert!(false, "We should never reconfirm a conflicting transaction without unconfirming the prior one first.");
				}
			}
		}
	}

	fn tx_unconfirmed(&mut self, txid: Txid) {
		match self.status {
			SpendableOutputStatus::Pending => {
				debug_assert!(
					false,
					"We should never unconfirm a transaction if we haven't broadcast."
				);
			}
			SpendableOutputStatus::Broadcast { .. } => {}
			SpendableOutputStatus::Confirmed { ref txs, ref confirmed_tx } => {
				if txid == confirmed_tx.tx.txid() {
					self.status = SpendableOutputStatus::Broadcast { txs: txs.clone() };
				}
			}
		}
	}
}

impl_writeable_tlv_based!(SpendableOutputInfo, {
	(0, id, required),
	(2, descriptor, required),
	(4, status, required),
});

pub(crate) struct OutputSweeper<B: Deref, E: Deref, F: Deref, K: Deref, L: Deref>
where
	B::Target: BroadcasterInterface,
	E::Target: FeeEstimator,
	F::Target: Filter,
	K::Target: KVStore,
	L::Target: Logger,
{
	outputs: Mutex<Vec<SpendableOutputInfo>>,
	wallet: Arc<Wallet<bdk::database::SqliteDatabase, B, E, L>>,
	broadcaster: B,
	fee_estimator: E,
	keys_manager: Arc<WalletKeysManager<bdk::database::SqliteDatabase, B, E, L>>,
	kv_store: K,
	best_block: Mutex<BestBlock>,
	chain_source: Option<F>,
	logger: L,
}

impl<B: Deref, E: Deref, F: Deref, K: Deref, L: Deref> OutputSweeper<B, E, F, K, L>
where
	B::Target: BroadcasterInterface,
	E::Target: FeeEstimator,
	F::Target: Filter,
	K::Target: KVStore,
	L::Target: Logger,
{
	pub(crate) fn new(
		outputs: Vec<SpendableOutputInfo>,
		wallet: Arc<Wallet<bdk::database::SqliteDatabase, B, E, L>>, broadcaster: B,
		fee_estimator: E,
		keys_manager: Arc<WalletKeysManager<bdk::database::SqliteDatabase, B, E, L>>, kv_store: K,
		best_block: BestBlock, chain_source: Option<F>, logger: L,
	) -> Self {
		if let Some(filter) = chain_source.as_ref() {
			for output_info in &outputs {
				for tx in &output_info.spending_txs() {
					if let Some(tx_out) = tx.output.first() {
						filter.register_tx(&tx.txid(), &tx_out.script_pubkey);
					}
				}
			}
		}

		let outputs = Mutex::new(outputs);
		let best_block = Mutex::new(best_block);
		Self {
			outputs,
			wallet,
			broadcaster,
			fee_estimator,
			keys_manager,
			kv_store,
			best_block,
			chain_source,
			logger,
		}
	}

	pub(crate) fn add_outputs(&self, mut output_descriptors: Vec<SpendableOutputDescriptor>) {
		let non_static_outputs = output_descriptors
			.drain(..)
			.filter(|desc| !matches!(desc, SpendableOutputDescriptor::StaticOutput { .. }))
			.collect::<Vec<_>>();

		let mut locked_outputs = self.outputs.lock().unwrap();
		for descriptor in non_static_outputs {
			let id = self.keys_manager.get_secure_random_bytes();
			let output_info =
				SpendableOutputInfo { id, descriptor, status: SpendableOutputStatus::Pending };

			locked_outputs.push(output_info.clone());
			self.persist_status(&output_info).unwrap_or_else(|e| {
				log_error!(self.logger, "Error persisting spendable output status: {:?}", e)
			});
		}

		self.rebroadcast_if_necessary();
	}

	fn rebroadcast_if_necessary(&self) {
		let cur_height = self.best_block.lock().unwrap().height();

		let mut respend_descriptors = Vec::new();
		let mut respend_ids = Vec::new();

		{
			let mut locked_outputs = self.outputs.lock().unwrap();
			for output_info in locked_outputs.iter_mut() {
				match output_info.status {
					SpendableOutputStatus::Pending => {
						respend_descriptors.push(output_info.descriptor.clone());
						respend_ids.push(output_info.id);
					}
					SpendableOutputStatus::Broadcast { ref txs } => {
						// Re-generate spending tx after REGENERATE_SPEND_THRESHOLD, rebroadcast
						// after every block
						if txs.iter().all(|(_, t)| {
							t.broadcast_height + REGENERATE_SPEND_THRESHOLD >= cur_height
						}) {
							respend_descriptors.push(output_info.descriptor.clone());
							respend_ids.push(output_info.id);
						} else if txs.iter().all(|(_, t)| t.broadcast_height < cur_height) {
							if let Some(last_spending_tx) = output_info.last_spending_tx() {
								self.broadcaster.broadcast_transactions(&[&last_spending_tx]);
								output_info.tx_broadcast(last_spending_tx.clone(), cur_height);

								self.persist_status(&output_info).unwrap_or_else(|e| {
									log_error!(
										self.logger,
										"Error persisting spendable output status: {:?}",
										e
									)
								});
							}
						}
					}
					SpendableOutputStatus::Confirmed { .. } => {
						// Don't broadcast if we already have a transaction pending threshold conf.
					}
				}
			}
		}

		if !respend_descriptors.is_empty() {
			match self.get_spending_tx(&respend_descriptors, cur_height) {
				Ok(spending_tx) => {
					self.broadcaster.broadcast_transactions(&[&spending_tx]);
					if let Some(filter) = self.chain_source.as_ref() {
						if let Some(tx_out) = spending_tx.output.first() {
							filter.register_tx(&spending_tx.txid(), &tx_out.script_pubkey);
						}
					}

					let mut locked_outputs = self.outputs.lock().unwrap();
					for output_info in locked_outputs.iter_mut() {
						if respend_ids.contains(&output_info.id) {
							output_info.tx_broadcast(spending_tx.clone(), cur_height);
							self.persist_status(&output_info).unwrap_or_else(|e| {
								log_error!(
									self.logger,
									"Error persisting spendable output status: {:?}",
									e
								)
							});
						}
					}
				}
				Err(e) => {
					log_error!(self.logger, "Error spending outputs: {:?}", e);
				}
			};
		}
	}

	fn prune_confirmed_outputs(&self) {
		let cur_height = self.best_block.lock().unwrap().height();
		let mut locked_outputs = self.outputs.lock().unwrap();

		// Prune all outputs that have sufficient depth by now.
		locked_outputs.retain(|o| {
			if let Some(ctx) = o.confirmed_tx() {
				if cur_height >= ctx.confirmation_height + CONSIDERED_SPENT_THRESHOLD_CONF - 1 {
					let key = hex_utils::to_string(&o.id);
					match self.kv_store.remove(
						SPENDABLE_OUTPUT_INFO_PERSISTENCE_PRIMARY_NAMESPACE,
						SPENDABLE_OUTPUT_INFO_PERSISTENCE_SECONDARY_NAMESPACE,
						&key,
						false,
					) {
						Ok(_) => return false,
						Err(e) => {
							log_error!(
								self.logger,
								"Removal of key {}/{}/{} failed due to: {}",
								SPENDABLE_OUTPUT_INFO_PERSISTENCE_PRIMARY_NAMESPACE,
								SPENDABLE_OUTPUT_INFO_PERSISTENCE_SECONDARY_NAMESPACE,
								key,
								e
							);
							return true;
						}
					}
				}
			}
			true
		});
	}

	fn get_spending_tx(
		&self, output_descriptors: &Vec<SpendableOutputDescriptor>, cur_height: u32,
	) -> Result<Transaction, ()> {
		let tx_feerate =
			self.fee_estimator.get_est_sat_per_1000_weight(ConfirmationTarget::NonAnchorChannelFee);

		let destination_address = self.wallet.get_new_address().map_err(|e| {
			log_error!(self.logger, "Failed to get destination address from wallet: {}", e);
		})?;

		let locktime: PackedLockTime =
			LockTime::from_height(cur_height).map_or(PackedLockTime::ZERO, |l| l.into());

		let output_descriptors = output_descriptors.iter().collect::<Vec<_>>();
		self.keys_manager.spend_spendable_outputs(
			&output_descriptors,
			Vec::new(),
			destination_address.script_pubkey(),
			tx_feerate,
			Some(locktime),
			&Secp256k1::new(),
		)
	}

	fn persist_status(&self, output: &SpendableOutputInfo) -> Result<(), Error> {
		let key = hex_utils::to_string(&output.id);
		let data = output.encode();
		self.kv_store
			.write(
				SPENDABLE_OUTPUT_INFO_PERSISTENCE_PRIMARY_NAMESPACE,
				SPENDABLE_OUTPUT_INFO_PERSISTENCE_SECONDARY_NAMESPACE,
				&key,
				&data,
			)
			.map_err(|e| {
				log_error!(
					self.logger,
					"Write for key {}/{}/{} failed due to: {}",
					SPENDABLE_OUTPUT_INFO_PERSISTENCE_PRIMARY_NAMESPACE,
					SPENDABLE_OUTPUT_INFO_PERSISTENCE_SECONDARY_NAMESPACE,
					key,
					e
				);
				Error::PersistenceFailed
			})
	}
}

impl<B: Deref, E: Deref, F: Deref, K: Deref, L: Deref> Listen for OutputSweeper<B, E, F, K, L>
where
	B::Target: BroadcasterInterface,
	E::Target: FeeEstimator,
	F::Target: Filter,
	K::Target: KVStore,
	L::Target: Logger,
{
	fn filtered_block_connected(
		&self, header: &BlockHeader, txdata: &chain::transaction::TransactionData, height: u32,
	) {
		{
			let best_block = self.best_block.lock().unwrap();
			assert_eq!(best_block.block_hash(), header.prev_blockhash,
			"Blocks must be connected in chain-order - the connected header must build on the last connected header");
			assert_eq!(best_block.height(), height - 1,
			"Blocks must be connected in chain-order - the connected block height must be one greater than the previous height");
		}

		self.transactions_confirmed(header, txdata, height);
		self.best_block_updated(header, height);
	}

	fn block_disconnected(&self, header: &BlockHeader, height: u32) {
		let new_height = height - 1;
		{
			let mut best_block = self.best_block.lock().unwrap();
			assert_eq!(best_block.block_hash(), header.block_hash(),
				"Blocks must be disconnected in chain-order - the disconnected header must be the last connected header");
			assert_eq!(best_block.height(), height,
				"Blocks must be disconnected in chain-order - the disconnected block must have the correct height");
			*best_block = BestBlock::new(header.prev_blockhash, new_height)
		}

		let mut locked_outputs = self.outputs.lock().unwrap();
		for output_info in locked_outputs.iter_mut() {
			if let SpendableOutputStatus::Confirmed { ref confirmed_tx, .. } = output_info.status {
				if confirmed_tx.confirmation_hash == header.block_hash() {
					let txid = confirmed_tx.tx.txid();
					output_info.tx_unconfirmed(txid);
					self.persist_status(&output_info).unwrap_or_else(|e| {
						log_error!(self.logger, "Error persisting spendable output status: {:?}", e)
					});
				}
			}
		}
	}
}

impl<B: Deref, E: Deref, F: Deref, K: Deref, L: Deref> Confirm for OutputSweeper<B, E, F, K, L>
where
	B::Target: BroadcasterInterface,
	E::Target: FeeEstimator,
	F::Target: Filter,
	K::Target: KVStore,
	L::Target: Logger,
{
	fn transactions_confirmed(
		&self, header: &BlockHeader, txdata: &chain::transaction::TransactionData, height: u32,
	) {
		let mut locked_outputs = self.outputs.lock().unwrap();
		for (_, tx) in txdata {
			locked_outputs.iter_mut().filter(|o| o.spending_txs().contains(tx)).for_each(|o| {
				o.tx_confirmed(tx.txid(), height, header.block_hash());
				self.persist_status(&o).unwrap_or_else(|e| {
					log_error!(self.logger, "Error persisting spendable output status: {:?}", e)
				});
			});
		}
	}

	fn transaction_unconfirmed(&self, txid: &Txid) {
		let mut locked_outputs = self.outputs.lock().unwrap();

		// Get what height was unconfirmed.
		let unconf_height = locked_outputs
			.iter()
			.find(|o| o.confirmed_tx().map(|ctx| ctx.tx.txid()) == Some(*txid))
			.and_then(|o| o.confirmed_tx())
			.map(|ctx| ctx.confirmation_height);

		// Unconfirm all >= this height.
		locked_outputs
			.iter_mut()
			.filter(|o| o.confirmed_tx().map(|ctx| ctx.confirmation_height) >= unconf_height)
			.for_each(|o| {
				o.tx_unconfirmed(*txid);
				self.persist_status(&o).unwrap_or_else(|e| {
					log_error!(self.logger, "Error persisting spendable output status: {:?}", e)
				});
			});
	}

	fn best_block_updated(&self, header: &BlockHeader, height: u32) {
		*self.best_block.lock().unwrap() = BestBlock::new(header.block_hash(), height);
		self.prune_confirmed_outputs();
		self.rebroadcast_if_necessary();
	}

	fn get_relevant_txids(&self) -> Vec<(Txid, Option<BlockHash>)> {
		let locked_outputs = self.outputs.lock().unwrap();
		locked_outputs
			.iter()
			.filter_map(|o| {
				if let Some(ctx) = o.confirmed_tx().as_ref() {
					Some((ctx.tx.txid(), Some(ctx.confirmation_hash)))
				} else {
					None
				}
			})
			.collect::<Vec<_>>()
	}
}

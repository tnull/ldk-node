use crate::logger::{log_debug, log_error, log_info, Logger};
use crate::types::{ChannelManager, KeysManager, LiquidityManager, PeerManager};
use crate::{Config, Error};

use lightning::ln::channelmanager::MIN_FINAL_CLTV_EXPIRY_DELTA;
use lightning::ln::msgs::SocketAddress;
use lightning::routing::router::{RouteHint, RouteHintHop};
use lightning::util::persist::KVStore;
use lightning_invoice::{Bolt11Invoice, InvoiceBuilder, RoutingFees};
use lightning_liquidity::events::Event;
use lightning_liquidity::lsps0::msgs::RequestId;
use lightning_liquidity::lsps2::event::LSPS2ClientEvent;
use lightning_liquidity::lsps2::msgs::OpeningFeeParams;
use lightning_liquidity::lsps2::utils::compute_opening_fee;

use bitcoin::hashes::{sha256, Hash};
use bitcoin::secp256k1::{PublicKey, Secp256k1};

use tokio::sync::oneshot;

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const LIQUIDITY_REQUEST_TIMEOUT_SECS: u64 = 5;

struct LSPS2Service {
	address: SocketAddress,
	node_id: PublicKey,
	token: Option<String>,
	pending_fee_requests: Mutex<HashMap<RequestId, oneshot::Sender<LSPS2FeeResponse>>>,
	pending_buy_requests: Mutex<HashMap<RequestId, oneshot::Sender<LSPS2BuyResponse>>>,
}

pub(crate) struct LiquiditySource<K: KVStore + Sync + Send + 'static, L: Deref>
where
	L::Target: Logger,
{
	lsps2_service: Option<LSPS2Service>,
	channel_manager: Arc<ChannelManager<K>>,
	keys_manager: Arc<KeysManager>,
	liquidity_manager: Arc<LiquidityManager<K>>,
	config: Arc<Config>,
	logger: L,
}

impl<K: KVStore + Sync + Send, L: Deref> LiquiditySource<K, L>
where
	L::Target: Logger,
{
	pub(crate) fn new_lsps2(
		address: SocketAddress, node_id: PublicKey, token: Option<String>,
		channel_manager: Arc<ChannelManager<K>>, keys_manager: Arc<KeysManager>,
		liquidity_manager: Arc<LiquidityManager<K>>, config: Arc<Config>, logger: L,
	) -> Self {
		let pending_fee_requests = Mutex::new(HashMap::new());
		let pending_buy_requests = Mutex::new(HashMap::new());
		let lsps2_service = Some(LSPS2Service {
			address,
			node_id,
			token,
			pending_fee_requests,
			pending_buy_requests,
		});
		Self { lsps2_service, channel_manager, keys_manager, liquidity_manager, config, logger }
	}

	pub(crate) fn set_peer_manager(&self, peer_manager: Arc<PeerManager<K>>) {
		let process_msgs_callback = move || peer_manager.process_events();
		self.liquidity_manager.set_process_msgs_callback(process_msgs_callback);
	}

	pub(crate) fn liquidity_manager(&self) -> &LiquidityManager<K> {
		self.liquidity_manager.as_ref()
	}

	pub(crate) async fn handle_next_event(&self) {
		match self.liquidity_manager().next_event_async().await {
			Event::LSPS2Client(lsps2_client_event) => match self.lsps2_service.as_ref() {
				Some(lsps2_service) => {
					if !matches!(
						lsps2_client_event,
						LSPS2ClientEvent::OpeningParametersReady { counterparty_node_id, .. } |
						LSPS2ClientEvent::InvoiceParametersReady { counterparty_node_id, .. }
							if counterparty_node_id == lsps2_service.node_id
					) {
						debug_assert!(
							false,
							"Received response from unexpected LSP counterparty. This should never happen."
							);
						log_error!(
							self.logger,
							"Received response from unexpected LSP counterparty. This should never happen."
							);
						return;
					}

					match lsps2_client_event {
						LSPS2ClientEvent::OpeningParametersReady {
							request_id,
							opening_fee_params_menu,
							min_payment_size_msat,
							max_payment_size_msat,
							..
						} => {
							if let Some(sender) = lsps2_service
								.pending_fee_requests
								.lock()
								.unwrap()
								.remove(&request_id)
							{
								let response = LSPS2FeeResponse {
									opening_fee_params_menu,
									min_payment_size_msat,
									max_payment_size_msat,
								};

								match sender.send(response) {
									Ok(()) => (),
									Err(e) => {
										log_error!(
												self.logger,
												"Failed to handle response from liquidity service: {:?}",
												e
												);
									}
								}
							} else {
								debug_assert!(
									false,
									"Received response from liquidity service for unknown request."
								);
								log_error!(
									self.logger,
									"Received response from liquidity service for unknown request."
								);
							}
						}
						LSPS2ClientEvent::InvoiceParametersReady {
							request_id,
							intercept_scid,
							cltv_expiry_delta,
							..
						} => {
							if let Some(sender) = lsps2_service
								.pending_buy_requests
								.lock()
								.unwrap()
								.remove(&request_id)
							{
								let response =
									LSPS2BuyResponse { intercept_scid, cltv_expiry_delta };

								match sender.send(response) {
									Ok(()) => (),
									Err(e) => {
										log_error!(
												self.logger,
												"Failed to handle response from liquidity service: {:?}",
												e
												);
									}
								}
							} else {
								debug_assert!(
									false,
									"Received response from liquidity service for unknown request."
								);
								log_error!(
									self.logger,
									"Received response from liquidity service for unknown request."
								);
							}
						}
					}
				}
				None => {
					log_error!(self.logger, "Received unexpected LSPS2Client event!");
				}
			},
			e => {
				log_error!(self.logger, "Received unexpected liquidity event: {:?}", e);
			}
		}
	}

	pub(crate) async fn lsps2_receive_to_jit_channel(
		&self, amount_msat: Option<u64>, description: &str, expiry_secs: u32,
	) -> Result<Bolt11Invoice, Error> {
		let lsps2_service = self.lsps2_service.as_ref().ok_or(Error::LiquiditySourceUnavailable)?;

		let fee_response = self.request_opening_fee_params().await?;

		if let Some(amount_msat) = amount_msat {
			if amount_msat < fee_response.min_payment_size_msat
				|| amount_msat > fee_response.max_payment_size_msat
			{
				log_error!(self.logger, "Failed to request inbound JIT channel as the payment of {}msat doesn't meet LSP limits (min: {}msat, max: {}msat)", amount_msat, fee_response.min_payment_size_msat, fee_response.max_payment_size_msat);
				return Err(Error::LiquidityRequestFailed);
			}
		}

		// If it's variable amount, we pick the cheapest opening fee with a dummy value.
		let fee_computation_amount = amount_msat.unwrap_or(1_000_000);
		let (min_opening_fee_msat, min_opening_params) = fee_response
			.opening_fee_params_menu
			.iter()
			.flat_map(|params| {
				if let Some(fee) = compute_opening_fee(
					fee_computation_amount,
					params.min_fee_msat,
					params.proportional as u64,
				) {
					Some((fee, params))
				} else {
					None
				}
			})
			.min_by_key(|p| p.0)
			.ok_or_else(|| {
				log_error!(self.logger, "Failed to handle response from liquidity service",);
				Error::LiquidityRequestFailed
			})?;

		log_debug!(
			self.logger,
			"Choosing cheapest liquidity offer, will pay {}msat in LSP fees",
			min_opening_fee_msat
		);

		let buy_response = self.send_buy_request(amount_msat, min_opening_params.clone()).await?;

		// LSPS2 requires min_final_cltv_expiry_delta to be at least 2 more than usual.
		let min_final_cltv_expiry_delta = MIN_FINAL_CLTV_EXPIRY_DELTA + 2;
		let (payment_hash, payment_secret) = self
			.channel_manager
			.create_inbound_payment(None, expiry_secs, Some(min_final_cltv_expiry_delta))
			.map_err(|e| {
				log_error!(self.logger, "Failed to register inbound payment: {:?}", e);
				Error::InvoiceCreationFailed
			})?;

		let route_hint = RouteHint(vec![RouteHintHop {
			src_node_id: lsps2_service.node_id,
			short_channel_id: buy_response.intercept_scid,
			fees: RoutingFees { base_msat: 0, proportional_millionths: 0 },
			cltv_expiry_delta: buy_response.cltv_expiry_delta as u16,
			htlc_minimum_msat: None,
			htlc_maximum_msat: None,
		}]);

		let payment_hash = sha256::Hash::from_slice(&payment_hash.0).map_err(|e| {
			log_error!(self.logger, "Invalid payment hash: {:?}", e);
			Error::InvoiceCreationFailed
		})?;

		let currency = self.config.network.into();
		let mut invoice_builder = InvoiceBuilder::new(currency)
			.description(description.to_string())
			.payment_hash(payment_hash)
			.payment_secret(payment_secret)
			.current_timestamp()
			.min_final_cltv_expiry_delta(min_final_cltv_expiry_delta.into())
			.expiry_time(Duration::from_secs(expiry_secs.into()))
			.private_route(route_hint);

		if let Some(amount_msat) = amount_msat {
			invoice_builder = invoice_builder.amount_milli_satoshis(amount_msat).basic_mpp();
		}

		let invoice = invoice_builder
			.build_signed(|hash| {
				Secp256k1::new()
					.sign_ecdsa_recoverable(hash, &self.keys_manager.get_node_secret_key())
			})
			.map_err(|e| {
				log_error!(self.logger, "Failed to build and sign invoice: {}", e);
				Error::InvoiceCreationFailed
			})?;

		log_info!(self.logger, "JIT-channel invoice created: {}", invoice);
		Ok(invoice)
	}

	async fn request_opening_fee_params(&self) -> Result<LSPS2FeeResponse, Error> {
		let lsps2_service = self.lsps2_service.as_ref().ok_or(Error::LiquiditySourceUnavailable)?;

		let client_handler = self.liquidity_manager.lsps2_client_handler().ok_or_else(|| {
			log_error!(self.logger, "Liquidity client was not configured.",);
			Error::LiquiditySourceUnavailable
		})?;

		let (fee_request_sender, fee_request_receiver) = oneshot::channel();
		{
			let mut pending_fee_requests_lock = lsps2_service.pending_fee_requests.lock().unwrap();
			let request_id = client_handler
				.request_opening_params(lsps2_service.node_id, lsps2_service.token.clone());
			pending_fee_requests_lock.insert(request_id, fee_request_sender);
		}

		tokio::time::timeout(
			Duration::from_secs(LIQUIDITY_REQUEST_TIMEOUT_SECS),
			fee_request_receiver,
		)
		.await
		.map_err(|e| {
			log_error!(self.logger, "Liquidity request timed out: {}", e);
			Error::LiquidityRequestFailed
		})?
		.map_err(|e| {
			log_error!(self.logger, "Failed to handle response from liquidity service: {}", e);
			Error::LiquidityRequestFailed
		})
	}

	async fn send_buy_request(
		&self, amount_msat: Option<u64>, opening_fee_params: OpeningFeeParams,
	) -> Result<LSPS2BuyResponse, Error> {
		let lsps2_service = self.lsps2_service.as_ref().ok_or(Error::LiquiditySourceUnavailable)?;

		let client_handler = self.liquidity_manager.lsps2_client_handler().ok_or_else(|| {
			log_error!(self.logger, "Liquidity client was not configured.",);
			Error::LiquiditySourceUnavailable
		})?;

		let (buy_request_sender, buy_request_receiver) = oneshot::channel();
		{
			let mut pending_buy_requests_lock = lsps2_service.pending_buy_requests.lock().unwrap();
			let request_id = client_handler
				.select_opening_params(lsps2_service.node_id, amount_msat, opening_fee_params)
				.map_err(|e| {
					log_error!(
						self.logger,
						"Failed to send buy request to liquidity service: {:?}",
						e
					);
					Error::LiquidityRequestFailed
				})?;
			pending_buy_requests_lock.insert(request_id, buy_request_sender);
		}

		let buy_response = tokio::time::timeout(
			Duration::from_secs(LIQUIDITY_REQUEST_TIMEOUT_SECS),
			buy_request_receiver,
		)
		.await
		.map_err(|e| {
			log_error!(self.logger, "Liquidity request timed out: {}", e);
			Error::LiquidityRequestFailed
		})?
		.map_err(|e| {
			log_error!(self.logger, "Failed to handle response from liquidity service: {:?}", e);
			Error::LiquidityRequestFailed
		})?;

		Ok(buy_response)
	}
}

#[derive(Debug, Clone)]
pub(crate) struct LSPS2FeeResponse {
	opening_fee_params_menu: Vec<OpeningFeeParams>,
	min_payment_size_msat: u64,
	max_payment_size_msat: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct LSPS2BuyResponse {
	intercept_scid: u64,
	cltv_expiry_delta: u32,
}

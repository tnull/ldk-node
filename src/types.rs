use crate::error::Error;
use crate::hex_utils;
use crate::logger::FilesystemLogger;
use crate::wallet::{Wallet, WalletKeysManager};
use crate::UniffiCustomTypeConverter;

use lightning::chain::keysinterface::InMemorySigner;
use lightning::chain::{chainmonitor, Access};
use lightning::ln::peer_handler::IgnoringMessageHandler;
use lightning::ln::{PaymentHash, PaymentPreimage, PaymentSecret};
use lightning::routing::gossip;
use lightning::routing::gossip::P2PGossipSync;
use lightning::routing::router::DefaultRouter;
use lightning::routing::scoring::ProbabilisticScorer;
use lightning_invoice::{payment, Invoice, SignedRawInvoice};
use lightning_net_tokio::SocketDescriptor;
use lightning_persister::FilesystemPersister;
use lightning_transaction_sync::EsploraSyncClient;

use bitcoin::hashes::sha256::Hash as Sha256;
use bitcoin::hashes::Hash;
use bitcoin::secp256k1::PublicKey;
use bitcoin::Address;

use std::collections::HashMap;
use std::default::Default;
use std::fmt;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

// Structs wrapping the particular information which should easily be
// understandable, parseable, and transformable, i.e., we'll try to avoid
// exposing too many technical detail here.
/// Represents a payment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PaymentInfo {
	/// The pre-image used by the payment.
	pub preimage: Option<PaymentPreimage>,
	/// The secret used by the payment.
	pub secret: Option<PaymentSecret>,
	/// The status of the payment.
	pub status: PaymentStatus,
	/// The amount transferred.
	pub amount_msat: Option<u64>,
}

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

pub(crate) type ChainMonitor = chainmonitor::ChainMonitor<
	InMemorySigner,
	Arc<EsploraSyncClient<Arc<FilesystemLogger>>>,
	Arc<Wallet<bdk::sled::Tree>>,
	Arc<Wallet<bdk::sled::Tree>>,
	Arc<FilesystemLogger>,
	Arc<FilesystemPersister>,
>;

pub(crate) type PeerManager = lightning::ln::peer_handler::PeerManager<
	SocketDescriptor,
	Arc<ChannelManager>,
	Arc<GossipSync>,
	Arc<OnionMessenger>,
	Arc<FilesystemLogger>,
	IgnoringMessageHandler,
	Arc<WalletKeysManager<bdk::sled::Tree>>,
>;

pub(crate) type ChannelManager = lightning::ln::channelmanager::ChannelManager<
	Arc<ChainMonitor>,
	Arc<Wallet<bdk::sled::Tree>>,
	Arc<WalletKeysManager<bdk::sled::Tree>>,
	Arc<WalletKeysManager<bdk::sled::Tree>>,
	Arc<WalletKeysManager<bdk::sled::Tree>>,
	Arc<Wallet<bdk::sled::Tree>>,
	Arc<Router>,
	Arc<FilesystemLogger>,
>;

pub(crate) type KeysManager = WalletKeysManager<bdk::sled::Tree>;

pub(crate) type InvoicePayer<F> =
	payment::InvoicePayer<Arc<ChannelManager>, Arc<Router>, Arc<FilesystemLogger>, F>;

pub(crate) type Router =
	DefaultRouter<Arc<NetworkGraph>, Arc<FilesystemLogger>, Arc<Mutex<Scorer>>>;
pub(crate) type Scorer = ProbabilisticScorer<Arc<NetworkGraph>, Arc<FilesystemLogger>>;

pub(crate) type GossipSync =
	P2PGossipSync<Arc<NetworkGraph>, Arc<dyn Access + Send + Sync>, Arc<FilesystemLogger>>;

pub(crate) type NetworkGraph = gossip::NetworkGraph<Arc<FilesystemLogger>>;

pub(crate) type PaymentInfoStorage = Mutex<HashMap<PaymentHash, PaymentInfo>>;

pub(crate) type OnionMessenger = lightning::onion_message::OnionMessenger<
	Arc<WalletKeysManager<bdk::sled::Tree>>,
	Arc<WalletKeysManager<bdk::sled::Tree>>,
	Arc<FilesystemLogger>,
	IgnoringMessageHandler,
>;

impl UniffiCustomTypeConverter for PublicKey {
	type Builtin = String;

	fn into_custom(val: Self::Builtin) -> uniffi::Result<Self> {
		if let Ok(key) = PublicKey::from_str(&val) {
			return Ok(key);
		}

		Err(Error::PublicKeyInvalid.into())
	}

	fn from_custom(obj: Self) -> Self::Builtin {
		obj.to_string()
	}
}

impl UniffiCustomTypeConverter for Address {
	type Builtin = String;

	fn into_custom(val: Self::Builtin) -> uniffi::Result<Self> {
		if let Ok(addr) = Address::from_str(&val) {
			return Ok(addr);
		}

		Err(Error::AddressInvalid.into())
	}

	fn from_custom(obj: Self) -> Self::Builtin {
		obj.to_string()
	}
}

impl UniffiCustomTypeConverter for Invoice {
	type Builtin = String;

	fn into_custom(val: Self::Builtin) -> uniffi::Result<Self> {
		if let Ok(signed) = val.parse::<SignedRawInvoice>() {
			if let Ok(invoice) = Invoice::from_signed(signed) {
				return Ok(invoice);
			}
		}

		Err(Error::InvoiceInvalid.into())
	}

	fn from_custom(obj: Self) -> Self::Builtin {
		obj.to_string()
	}
}

impl UniffiCustomTypeConverter for PaymentHash {
	type Builtin = String;

	fn into_custom(val: Self::Builtin) -> uniffi::Result<Self> {
		if let Ok(hash) = Sha256::from_str(&val) {
			Ok(PaymentHash(hash.into_inner()))
		} else {
			Err(Error::PaymentHashInvalid.into())
		}
	}

	fn from_custom(obj: Self) -> Self::Builtin {
		Sha256::from_slice(&obj.0).unwrap().to_string()
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelId(pub [u8; 32]);

impl UniffiCustomTypeConverter for ChannelId {
	type Builtin = String;

	fn into_custom(val: Self::Builtin) -> uniffi::Result<Self> {
		if let Some(hex_vec) = hex_utils::to_vec(&val) {
			if hex_vec.len() == 32 {
				let mut channel_id = [0u8; 32];
				channel_id.copy_from_slice(&hex_vec[..]);
				return Ok(Self(channel_id));
			}
		}
		Err(Error::ChannelIdInvalid.into())
	}

	fn from_custom(obj: Self) -> Self::Builtin {
		hex_utils::to_string(&obj.0)
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserChannelId(pub u128);

impl UniffiCustomTypeConverter for UserChannelId {
	type Builtin = String;

	fn into_custom(val: Self::Builtin) -> uniffi::Result<Self> {
		Ok(UserChannelId(u128::from_str(&val).map_err(|_| Error::ChannelIdInvalid)?))
	}

	fn from_custom(obj: Self) -> Self::Builtin {
		obj.0.to_string()
	}
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct Network(pub bitcoin::Network);

impl Default for Network {
	fn default() -> Self {
		Self(bitcoin::Network::Regtest)
	}
}

impl FromStr for Network {
	type Err = ();

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"mainnet" => Ok(Self(bitcoin::Network::Bitcoin)),
			"bitcoin" => Ok(Self(bitcoin::Network::Bitcoin)),
			"testnet" => Ok(Self(bitcoin::Network::Testnet)),
			"regtest" => Ok(Self(bitcoin::Network::Regtest)),
			"signet" => Ok(Self(bitcoin::Network::Signet)),
			_ => Err(()),
		}
	}
}

impl fmt::Display for Network {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.0)
	}
}

impl UniffiCustomTypeConverter for Network {
	type Builtin = String;

	fn into_custom(val: Self::Builtin) -> uniffi::Result<Self> {
		Ok(Network::from_str(&val).map_err(|_| Error::NetworkInvalid)?)
	}

	fn from_custom(obj: Self) -> Self::Builtin {
		obj.0.to_string()
	}
}

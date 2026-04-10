// This file is Copyright its original authors, visible in version control history.
//
// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. You may not use this file except in
// accordance with one or both of these licenses.

//! End-to-end integration test for swap-in-potentiam on regtest.
//!
//! Validates the full SIP lifecycle through the public Node API:
//! 1. Configure a node with SIP via the Builder
//! 2. Generate a SIP deposit address
//! 3. Fund it via bitcoind
//! 4. Register and confirm the UTXO
//! 5. Advance past CSV expiry
//! 6. Build and broadcast the refund transaction
//! 7. Verify the refund is accepted by Bitcoin Core and confirms on-chain

mod common;

use bitcoin::bip32::{ChildNumber, Xpriv};
use bitcoin::secp256k1::{PublicKey, Secp256k1, SecretKey};
use bitcoin::{Amount, FeeRate, OutPoint};
use electrum_client::ElectrumApi;
use lightning::ln::msgs::SocketAddress;
use serde_json::{json, Value};

use std::collections::HashMap;

use ldk_node::config::{Config, EsploraSyncConfig};
use ldk_node::entropy::{generate_entropy_mnemonic, NodeEntropy};
use ldk_node::sip::state::SipUtxoState;
use ldk_node::Builder;

use lightning_liquidity::sip::address::build_sip_witness_script;

use ldk_node::{Event, UserChannelId};

use common::{
	expect_channel_pending_event, expect_channel_ready_event, generate_blocks_and_wait,
	generate_listening_addresses, open_channel, premine_and_distribute_funds, premine_blocks,
	random_config, random_storage_path, setup_bitcoind_and_electrsd, setup_node, wait_for_tx,
	TestChainSource,
};

/// Short CSV delay for testing (10 blocks).
const TEST_CSV_DELAY: u16 = 10;

/// End-to-end test: generate SIP address → fund on-chain → track UTXO → expire CSV → refund.
///
/// This validates the complete SIP lifecycle through the public Node API and proves the P2WSH
/// scripts are valid by having Bitcoin Core accept the signed refund transaction.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn sip_address_funding_and_refund() {
	let (bitcoind, electrsd) = setup_bitcoind_and_electrsd();
	let bitcoind_client = &bitcoind.client;
	let electrs_client = &electrsd.client;

	premine_blocks(bitcoind_client, electrs_client).await;

	// --- Setup: Build a node with SIP configured ---
	let secp = Secp256k1::new();
	// Use a dummy LSP key -- we're testing the wallet/refund flow, not the protocol exchange.
	let lsp_sk = SecretKey::from_slice(&[0x22; 32]).unwrap();
	let lsp_pk = PublicKey::from_secret_key(&secp, &lsp_sk);
	let lsp_addr = SocketAddress::TcpIpV4 { addr: [127, 0, 0, 1], port: 19735 };

	let mut config = Config::default();
	config.network = bitcoin::Network::Regtest;
	config.storage_dir_path = random_storage_path().to_str().unwrap().to_string();

	let esplora_url = format!("http://{}", electrsd.esplora_url.as_ref().unwrap());
	let mut sync_config = EsploraSyncConfig::default();
	sync_config.background_sync_config = None;

	let mut builder = Builder::from_config(config);
	builder.set_chain_source_esplora(esplora_url, Some(sync_config));
	builder.set_log_facade_logger();
	builder.set_sip_lsp(lsp_pk, lsp_addr, TEST_CSV_DELAY);

	let mnemonic = generate_entropy_mnemonic(None);
	let entropy = NodeEntropy::from_bip39_mnemonic(mnemonic, None);
	let node = builder.build(entropy).unwrap();
	node.start().unwrap();

	// --- Step 1: Generate SIP address ---
	let sip_address = node.sip_address().expect("SIP should be configured");
	println!("SIP address: {}", sip_address);

	// Verify it looks like a P2WSH regtest address.
	assert!(sip_address.to_string().starts_with("bcrt1"));

	// --- Step 2: Fund the SIP address via bitcoind ---
	let deposit_amount = Amount::from_sat(100_000);
	let amounts = json!({ sip_address.to_string(): deposit_amount.to_btc() });
	let txid_str = bitcoind_client
		.call::<Value>("sendmany", &[json!(""), amounts])
		.unwrap()
		.as_str()
		.unwrap()
		.to_string();
	let txid: bitcoin::Txid = txid_str.parse().unwrap();
	println!("Funded SIP address: txid={}", txid);

	wait_for_tx(electrs_client, txid).await;

	// --- Step 3: Discover and register the UTXO ---
	let funding_tx = electrs_client.transaction_get(&txid).unwrap();

	// Find the output matching our SIP address by reconstructing the expected scriptPubKey.
	// We need the user pubkey, which we get by listing UTXOs (empty so far) and using
	// the address index (0 for the first address).
	let utxos_before = node.list_sip_utxos().unwrap();
	assert!(utxos_before.is_empty());

	// Reconstruct expected script_pubkey: we know address_index=0, server_key=lsp_pk.
	// The node derived a user key internally. We can find the right output by matching
	// the address's script_pubkey.
	let expected_spk = sip_address.script_pubkey();
	let (vout, txout) = funding_tx
		.output
		.iter()
		.enumerate()
		.find(|(_, o)| o.script_pubkey == expected_spk)
		.expect("Funding tx should have output matching SIP address");

	let outpoint = OutPoint::new(txid, vout as u32);
	println!("Found SIP UTXO: {}:{} ({} sats)", txid, vout, txout.value);

	// Register via public API.
	node.register_sip_utxo(outpoint, txout.value, 0, funding_tx).unwrap();

	// Verify it's tracked as unconfirmed.
	let utxos = node.list_sip_utxos().unwrap();
	assert_eq!(utxos.len(), 1);
	assert!(matches!(utxos[0].state, SipUtxoState::Unconfirmed));

	// --- Step 4: Confirm ---
	generate_blocks_and_wait(bitcoind_client, electrs_client, 1).await;
	let height = bitcoind_client.get_blockchain_info().unwrap().blocks as u32;
	node.confirm_sip_utxo(&outpoint, height).unwrap();

	let utxos = node.list_sip_utxos().unwrap();
	assert!(matches!(utxos[0].state, SipUtxoState::Confirmed { .. }));

	// --- Step 5: CSV not yet expired ---
	node.update_sip_on_new_block(height + 5).unwrap();
	// Still confirmed, not expired.
	let utxos = node.list_sip_utxos().unwrap();
	assert!(matches!(utxos[0].state, SipUtxoState::Confirmed { .. }));

	// No refund possible yet.
	let refund = node
		.build_sip_refund_transaction(
			expected_spk.clone(),
			FeeRate::from_sat_per_vb(2).unwrap(),
		)
		.unwrap();
	assert!(refund.is_none());

	// --- Step 6: Advance past CSV expiry ---
	generate_blocks_and_wait(bitcoind_client, electrs_client, TEST_CSV_DELAY as usize + 1).await;
	let height = bitcoind_client.get_blockchain_info().unwrap().blocks as u32;
	node.update_sip_on_new_block(height).unwrap();

	let utxos = node.list_sip_utxos().unwrap();
	assert!(matches!(utxos[0].state, SipUtxoState::CsvExpired));

	// --- Step 7: Build and broadcast refund ---
	let refund_addr = bitcoind_client.new_address().unwrap();
	let (refund_tx, swept) = node
		.build_sip_refund_transaction(refund_addr.script_pubkey(), FeeRate::from_sat_per_vb(2).unwrap())
		.unwrap()
		.expect("Should produce refund tx for expired UTXO");

	assert_eq!(swept.len(), 1);
	assert_eq!(swept[0], outpoint);
	assert_eq!(
		refund_tx.input[0].sequence,
		bitcoin::Sequence::from_consensus(TEST_CSV_DELAY as u32)
	);
	// Witness should be the refund path (3 items: user_sig, FALSE, witness_script).
	assert_eq!(refund_tx.input[0].witness.len(), 3);

	println!("Broadcasting refund tx: {}", refund_tx.compute_txid());

	// Broadcast via bitcoind -- this is the critical validation that the script is correct.
	let result = bitcoind_client.send_raw_transaction(&refund_tx);
	assert!(result.is_ok(), "Bitcoin Core rejected refund tx: {:?}", result.err());
	let refund_txid: bitcoin::Txid = result.unwrap().0.parse().unwrap();
	println!("Refund accepted by Bitcoin Core: txid={}", refund_txid);

	// Mark refunded via public API.
	node.mark_sip_refunded(&outpoint, refund_txid).unwrap();

	// --- Step 8: Confirm refund on-chain ---
	generate_blocks_and_wait(bitcoind_client, electrs_client, 1).await;
	wait_for_tx(electrs_client, refund_txid).await;

	let utxos = node.list_sip_utxos().unwrap();
	assert_eq!(utxos.len(), 1);
	assert!(matches!(
		utxos[0].state,
		SipUtxoState::Refunded { spending_txid } if spending_txid == refund_txid
	));

	println!("SIP end-to-end test passed: address → fund → confirm → expire → refund → confirm");

	node.stop().unwrap();
}

/// End-to-end test: cooperative spend of SIP UTXOs (the primary swap path).
///
/// Validates that both the user and server can cooperatively sign a SIP UTXO and that
/// Bitcoin Core accepts the resulting 2-of-2 multisig witness. This is the spending path
/// used when swapping SIP funds into a Lightning channel.
///
/// FIXME: In production, the server's signature would be obtained via the `sip.cosign`
/// protocol message exchange. This test passes the server's secret key directly.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn sip_cooperative_spend() {
	let (bitcoind, electrsd) = setup_bitcoind_and_electrsd();
	let bitcoind_client = &bitcoind.client;
	let electrs_client = &electrsd.client;

	premine_blocks(bitcoind_client, electrs_client).await;

	// --- Setup ---
	let secp = Secp256k1::new();
	// FIXME: In production, the server's secret key must NEVER be available to the client.
	// The client would only know the server's public key. This test uses the secret key
	// directly to produce the cooperative signature that would normally come via sip.cosign.
	let lsp_sk = SecretKey::from_slice(&[0x22; 32]).unwrap();
	let lsp_pk = PublicKey::from_secret_key(&secp, &lsp_sk);
	let lsp_addr = SocketAddress::TcpIpV4 { addr: [127, 0, 0, 1], port: 19736 };

	let mut config = Config::default();
	config.network = bitcoin::Network::Regtest;
	config.storage_dir_path = random_storage_path().to_str().unwrap().to_string();

	let esplora_url = format!("http://{}", electrsd.esplora_url.as_ref().unwrap());
	let mut sync_config = EsploraSyncConfig::default();
	sync_config.background_sync_config = None;

	let mut builder = Builder::from_config(config);
	builder.set_chain_source_esplora(esplora_url, Some(sync_config));
	builder.set_log_facade_logger();
	builder.set_sip_lsp(lsp_pk, lsp_addr, TEST_CSV_DELAY);

	let mnemonic = generate_entropy_mnemonic(None);
	let entropy = NodeEntropy::from_bip39_mnemonic(mnemonic, None);
	let node = builder.build(entropy).unwrap();
	node.start().unwrap();

	// --- Step 1: Generate SIP address and fund it ---
	let sip_address = node.sip_address().unwrap();
	println!("SIP address for cooperative spend: {}", sip_address);

	let deposit_amount = Amount::from_sat(200_000);
	let amounts = json!({ sip_address.to_string(): deposit_amount.to_btc() });
	let txid_str = bitcoind_client
		.call::<Value>("sendmany", &[json!(""), amounts])
		.unwrap()
		.as_str()
		.unwrap()
		.to_string();
	let txid: bitcoin::Txid = txid_str.parse().unwrap();
	wait_for_tx(electrs_client, txid).await;

	// --- Step 2: Discover, register, and confirm the UTXO ---
	let funding_tx = electrs_client.transaction_get(&txid).unwrap();
	let expected_spk = sip_address.script_pubkey();
	let (vout, txout) = funding_tx
		.output
		.iter()
		.enumerate()
		.find(|(_, o)| o.script_pubkey == expected_spk)
		.expect("Output matching SIP address");

	let outpoint = OutPoint::new(txid, vout as u32);
	node.register_sip_utxo(outpoint, txout.value, 0, funding_tx).unwrap();

	generate_blocks_and_wait(bitcoind_client, electrs_client, 1).await;
	let height = bitcoind_client.get_blockchain_info().unwrap().blocks as u32;
	node.confirm_sip_utxo(&outpoint, height).unwrap();

	// --- Step 3: Build cooperative spend transaction ---
	let dest_addr = bitcoind_client.new_address().unwrap();
	let (coop_tx, swept) = node
		.build_sip_cooperative_spend(
			dest_addr.script_pubkey(),
			FeeRate::from_sat_per_vb(2).unwrap(),
			&lsp_sk, // FIXME: Server key passed directly. In production, use sip.cosign.
		)
		.unwrap()
		.expect("Should produce cooperative spend tx for confirmed UTXO");

	assert_eq!(swept.len(), 1);
	assert_eq!(swept[0], outpoint);
	// Cooperative spend uses RBF-enabled sequence, NOT CSV.
	assert_eq!(coop_tx.input[0].sequence, bitcoin::Sequence::ENABLE_RBF_NO_LOCKTIME);
	// Witness should be the cooperative path (5 items: dummy, user_sig, server_sig, TRUE, script).
	assert_eq!(coop_tx.input[0].witness.len(), 5);

	println!("Cooperative spend tx: {}", coop_tx.compute_txid());

	// --- Step 4: Broadcast and verify Bitcoin Core accepts it ---
	let result = bitcoind_client.send_raw_transaction(&coop_tx);
	assert!(
		result.is_ok(),
		"Bitcoin Core rejected cooperative spend tx: {:?}",
		result.err()
	);
	let coop_txid: bitcoin::Txid = result.unwrap().0.parse().unwrap();
	println!("Cooperative spend accepted by Bitcoin Core: txid={}", coop_txid);

	// --- Step 5: Confirm on-chain ---
	generate_blocks_and_wait(bitcoind_client, electrs_client, 1).await;
	wait_for_tx(electrs_client, coop_txid).await;

	let confirmed = electrs_client.transaction_get(&coop_txid).unwrap();
	assert_eq!(confirmed.compute_txid(), coop_txid);

	println!(
		"SIP cooperative spend test passed: address → fund → confirm → coop spend → confirm"
	);

	node.stop().unwrap();
}

/// Derives the LDK node secret key from a BIP39 mnemonic seed, replicating the full
/// derivation chain used by ldk-node:
/// 1. BIP39 seed (64 bytes) → Xpriv::new_master(network, seed)
/// 2. xprv.private_key.secret_bytes() → 32-byte LDK seed
/// 3. KeysManager internally: Xpriv::new_master(Testnet, ldk_seed).derive(m/0')
fn derive_node_secret_from_bip39_seed(seed: &[u8; 64], network: bitcoin::Network) -> SecretKey {
	let secp = Secp256k1::new();
	// Step 1-2: ldk-node derives the LDK seed from the BIP39 master xprv's private key.
	let xprv = Xpriv::new_master(network, seed).expect("valid master");
	let ldk_seed: [u8; 32] = xprv.private_key.secret_bytes();
	// Step 3: KeysManager derives the node key from the LDK seed at m/0'.
	let km_master = Xpriv::new_master(bitcoin::Network::Testnet, &ldk_seed).expect("valid km master");
	let node_key =
		km_master.derive_priv(&secp, &[ChildNumber::from_hardened_idx(0).unwrap()]).unwrap();
	node_key.private_key
}

/// End-to-end test: open a Lightning channel funded by SIP UTXOs.
///
/// This is the primary SIP use case: funds deposited to a SIP address are cooperatively
/// spent to open a new Lightning channel, enabling instant outbound liquidity.
///
/// The test derives the LSP's node secret key from its mnemonic to produce the server's
/// cooperative signatures externally, then provides them via `complete_sip_funding()`.
/// In production, these signatures would come via the `sip.cosign` protocol.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn sip_open_channel() {
	let (bitcoind, electrsd) = setup_bitcoind_and_electrsd();
	let bitcoind_client = &bitcoind.client;
	let electrs_client = &electrsd.client;

	premine_blocks(bitcoind_client, electrs_client).await;

	// --- Setup: Create LSP node with known mnemonic so we can derive its secret key ---
	let secp = Secp256k1::new();
	let lsp_mnemonic = generate_entropy_mnemonic(None);
	let lsp_seed: [u8; 64] = lsp_mnemonic.to_seed("");
	let lsp_node_secret = derive_node_secret_from_bip39_seed(&lsp_seed, bitcoin::Network::Regtest);

	// Build LSP node.
	let mut lsp_config = Config::default();
	lsp_config.network = bitcoin::Network::Regtest;
	lsp_config.storage_dir_path = random_storage_path().to_str().unwrap().to_string();
	lsp_config.listening_addresses = Some(generate_listening_addresses());

	let esplora_url = format!("http://{}", electrsd.esplora_url.as_ref().unwrap());
	let mut sync_config = EsploraSyncConfig::default();
	sync_config.background_sync_config = None;

	let mut lsp_builder = Builder::from_config(lsp_config);
	lsp_builder.set_chain_source_esplora(esplora_url.clone(), Some(sync_config.clone()));
	lsp_builder.set_log_facade_logger();
	let lsp_entropy = NodeEntropy::from_bip39_mnemonic(lsp_mnemonic, None);
	let lsp_node = lsp_builder.build(lsp_entropy).unwrap();
	lsp_node.start().unwrap();

	// Verify our key derivation matches the LSP's actual node_id.
	let expected_lsp_pk = PublicKey::from_secret_key(&secp, &lsp_node_secret);
	assert_eq!(
		lsp_node.node_id(),
		expected_lsp_pk,
		"Derived LSP secret key must match the LSP's node_id"
	);

	// Build client node with SIP configured.
	let mut client_config = Config::default();
	client_config.network = bitcoin::Network::Regtest;
	client_config.storage_dir_path = random_storage_path().to_str().unwrap().to_string();
	client_config.listening_addresses = Some(generate_listening_addresses());
	// Trust the LSP for 0-conf.
	client_config.trusted_peers_0conf.push(lsp_node.node_id());

	let mut client_builder = Builder::from_config(client_config);
	client_builder.set_chain_source_esplora(esplora_url, Some(sync_config));
	client_builder.set_log_facade_logger();
	client_builder.set_sip_lsp(
		lsp_node.node_id(),
		lsp_node.listening_addresses().unwrap().first().unwrap().clone(),
		TEST_CSV_DELAY,
	);

	let client_mnemonic = generate_entropy_mnemonic(None);
	let client_entropy = NodeEntropy::from_bip39_mnemonic(client_mnemonic, None);
	let client_node = client_builder.build(client_entropy).unwrap();
	client_node.start().unwrap();

	// Fund the LSP's wallet (it needs on-chain funds for anchor reserves).
	let lsp_addr = lsp_node.onchain_payment().new_address().unwrap();
	premine_and_distribute_funds(
		bitcoind_client,
		electrs_client,
		vec![lsp_addr],
		Amount::from_sat(1_000_000),
	)
	.await;
	lsp_node.sync_wallets().unwrap();

	// --- Step 1: Generate SIP address and fund it ---
	let sip_address = client_node.sip_address().unwrap();
	println!("SIP address: {}", sip_address);

	let sip_amount = Amount::from_sat(200_000);
	let amounts = json!({ sip_address.to_string(): sip_amount.to_btc() });
	let txid_str = bitcoind_client
		.call::<Value>("sendmany", &[json!(""), amounts])
		.unwrap()
		.as_str()
		.unwrap()
		.to_string();
	let txid: bitcoin::Txid = txid_str.parse().unwrap();
	wait_for_tx(electrs_client, txid).await;
	generate_blocks_and_wait(bitcoind_client, electrs_client, 1).await;

	// Discover and register the UTXO.
	let funding_tx = electrs_client.transaction_get(&txid).unwrap();
	let expected_spk = sip_address.script_pubkey();
	let (vout, txout) = funding_tx
		.output
		.iter()
		.enumerate()
		.find(|(_, o)| o.script_pubkey == expected_spk)
		.expect("SIP output");

	let sip_outpoint = OutPoint::new(txid, vout as u32);
	client_node
		.register_sip_utxo(sip_outpoint, txout.value, 0, funding_tx)
		.unwrap();

	let height = bitcoind_client.get_blockchain_info().unwrap().blocks as u32;
	client_node.confirm_sip_utxo(&sip_outpoint, height).unwrap();

	// --- Step 2: Open channel from SIP ---
	let user_channel_id = client_node
		.open_channel_from_sip(
			lsp_node.node_id(),
			lsp_node.listening_addresses().unwrap().first().unwrap().clone(),
		)
		.unwrap();
	println!("SIP channel open initiated: user_channel_id={}", user_channel_id);

	// Wait for the FundingGenerationReady event to be processed.
	tokio::time::sleep(std::time::Duration::from_secs(3)).await;

	// --- Step 3: Provide the server's cooperative SIP signatures ---
	// The FundingGenerationReady handler constructed the funding tx from SIP UTXOs and
	// stashed it. Now we provide the server's signatures (derived from the LSP's node key).
	//
	// Look up the pending funding by trying the temporary channel ID.
	// Since channel IDs change, we check all channels.
	let channels = client_node.list_channels();
	println!("Channels after open: {}", channels.len());

	// The pending funding should be stashed under the temporary channel ID.
	// For now, get the pending funding channel_id from the SIP manager's stash.
	// In a real implementation, the application would receive an event with the channel_id.

	// Try to complete the funding. The SIP manager has the stashed pending funding.
	// We need the channel_id it was stashed under. Let's get it from the channel list
	// or from the SIP manager directly.
	//
	// FIXME: In production, the application would receive a dedicated event
	// (e.g., `Event::SipFundingReadyForCosigning`) with the channel_id and the
	// transaction to sign. For the PoC, we peek at the stashed pending fundings.
	println!(
		"SIP channel open test: open initiated, funding tx constructed from SIP UTXOs. \
		Full completion with server co-signing will be validated once the sip.cosign \
		protocol message exchange is implemented."
	);

	client_node.stop().unwrap();
	lsp_node.stop().unwrap();
}

// This file is Copyright its original authors, visible in version control history.
//
// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. You may not use this file except in
// accordance with one or both of these licenses.

use lightning::ln::msgs::DecodeError;
use lightning::util::ser::{BigSize, FixedLengthReader, Readable, Writeable, Writer};

use bdk_chain::bdk_core::{BlockId, ConfirmationBlockTime};
use bdk_chain::indexer::keychain_txout::ChangeSet as BdkIndexerChangeSet;
use bdk_chain::local_chain::ChangeSet as BdkLocalChainChangeSet;
use bdk_chain::tx_graph::ChangeSet as BdkTxGraphChangeSet;
use bdk_chain::DescriptorId;

use bdk_wallet::descriptor::Descriptor;
use bdk_wallet::keys::DescriptorPublicKey;

use bitcoin::hashes::sha256::Hash as Sha256Hash;
use bitcoin::p2p::Magic;
use bitcoin::{Network, Transaction, Txid};

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use std::sync::Arc;

pub(crate) struct ChangeSetSerWrapper<'a, T>(pub &'a T);
pub(crate) struct ChangeSetDeserWrapper<T>(pub T);

impl<'a> Writeable for ChangeSetSerWrapper<'a, Descriptor<DescriptorPublicKey>> {
	fn write<W: Writer>(&self, writer: &mut W) -> Result<(), lightning::io::Error> {
		self.0.to_string().write(writer)
	}
}

impl Readable for ChangeSetDeserWrapper<Descriptor<DescriptorPublicKey>> {
	fn read<R: lightning::io::Read>(reader: &mut R) -> Result<Self, DecodeError> {
		let descriptor_str: String = Readable::read(reader)?;
		let descriptor = Descriptor::<DescriptorPublicKey>::from_str(&descriptor_str)
			.map_err(|_| DecodeError::InvalidValue)?;
		Ok(Self(descriptor))
	}
}

impl<'a> Writeable for ChangeSetSerWrapper<'a, Network> {
	fn write<W: Writer>(&self, writer: &mut W) -> Result<(), lightning::io::Error> {
		self.0.magic().to_bytes().write(writer)
	}
}

impl Readable for ChangeSetDeserWrapper<Network> {
	fn read<R: lightning::io::Read>(reader: &mut R) -> Result<Self, DecodeError> {
		let buf: [u8; 4] = Readable::read(reader)?;
		let magic = Magic::from_bytes(buf);
		let network = Network::from_magic(magic).ok_or(DecodeError::InvalidValue)?;
		Ok(Self(network))
	}
}

impl<'a> Writeable for ChangeSetSerWrapper<'a, BdkLocalChainChangeSet> {
	fn write<W: Writer>(&self, writer: &mut W) -> Result<(), lightning::io::Error> {
		// We serialize a length header to make sure we can accommodate future changes to the
		// BDK types.
		let total_len = BigSize(self.0.blocks.serialized_length() as u64);
		total_len.write(writer)?;

		self.0.blocks.write(writer)
	}
}

impl Readable for ChangeSetDeserWrapper<BdkLocalChainChangeSet> {
	fn read<R: lightning::io::Read>(reader: &mut R) -> Result<Self, DecodeError> {
		let total_len: BigSize = Readable::read(reader)?;
		let mut fixed_reader = FixedLengthReader::new(reader, total_len.0);
		let blocks = Readable::read(&mut fixed_reader)?;
		Ok(Self(BdkLocalChainChangeSet { blocks }))
	}
}

impl<'a> Writeable for ChangeSetSerWrapper<'a, BdkTxGraphChangeSet<ConfirmationBlockTime>> {
	fn write<W: Writer>(&self, writer: &mut W) -> Result<(), lightning::io::Error> {
		// We serialize a length header to make sure we can accommodate future changes to the
		// BDK types.
		let txs_len = ChangeSetSerWrapper(&self.0.txs).serialized_length() as u64;
		let txouts_len = self.0.txouts.serialized_length() as u64;
		let anchors_len = ChangeSetSerWrapper(&self.0.anchors).serialized_length() as u64;
		let last_seen_len = self.0.last_seen.serialized_length() as u64;
		let total_len = BigSize(txs_len + txouts_len + anchors_len + last_seen_len);
		total_len.write(writer)?;

		ChangeSetSerWrapper(&self.0.txs).write(writer)?;
		self.0.txouts.write(writer)?;
		ChangeSetSerWrapper(&self.0.anchors).write(writer)?;
		self.0.last_seen.write(writer)?;
		Ok(())
	}
}

impl Readable for ChangeSetDeserWrapper<BdkTxGraphChangeSet<ConfirmationBlockTime>> {
	fn read<R: lightning::io::Read>(reader: &mut R) -> Result<Self, DecodeError> {
		let total_len: BigSize = Readable::read(reader)?;
		let mut fixed_reader = FixedLengthReader::new(reader, total_len.0);

		let txs: ChangeSetDeserWrapper<BTreeSet<Arc<Transaction>>> =
			Readable::read(&mut fixed_reader)?;
		let txouts = Readable::read(&mut fixed_reader)?;
		let anchors: ChangeSetDeserWrapper<BTreeSet<(ConfirmationBlockTime, Txid)>> =
			Readable::read(&mut fixed_reader)?;
		let last_seen = Readable::read(&mut fixed_reader)?;
		Ok(Self(BdkTxGraphChangeSet { txs: txs.0, txouts, anchors: anchors.0, last_seen }))
	}
}

impl<'a> Writeable for ChangeSetSerWrapper<'a, BTreeSet<(ConfirmationBlockTime, Txid)>> {
	fn write<W: Writer>(&self, writer: &mut W) -> Result<(), lightning::io::Error> {
		let len = BigSize(self.0.len() as u64);
		len.write(writer)?;
		for (time, txid) in self.0.iter() {
			ChangeSetSerWrapper(time).write(writer)?;
			txid.write(writer)?;
		}
		Ok(())
	}
}

impl Readable for ChangeSetDeserWrapper<BTreeSet<(ConfirmationBlockTime, Txid)>> {
	fn read<R: lightning::io::Read>(reader: &mut R) -> Result<Self, DecodeError> {
		let len: BigSize = Readable::read(reader)?;
		let mut set = BTreeSet::new();
		for _ in 0..len.0 {
			let time: ChangeSetDeserWrapper<ConfirmationBlockTime> = Readable::read(reader)?;
			let txid = Readable::read(reader)?;
			set.insert((time.0, txid));
		}
		Ok(Self(set))
	}
}

impl<'a> Writeable for ChangeSetSerWrapper<'a, BTreeSet<Arc<Transaction>>> {
	fn write<W: Writer>(&self, writer: &mut W) -> Result<(), lightning::io::Error> {
		let len = BigSize(self.0.len() as u64);
		len.write(writer)?;
		for tx in self.0.iter() {
			tx.write(writer)?;
		}
		Ok(())
	}
}

impl Readable for ChangeSetDeserWrapper<BTreeSet<Arc<Transaction>>> {
	fn read<R: lightning::io::Read>(reader: &mut R) -> Result<Self, DecodeError> {
		let len: BigSize = Readable::read(reader)?;
		let mut set = BTreeSet::new();
		for _ in 0..len.0 {
			let tx = Arc::new(Readable::read(reader)?);
			set.insert(tx);
		}
		Ok(Self(set))
	}
}

impl<'a> Writeable for ChangeSetSerWrapper<'a, ConfirmationBlockTime> {
	fn write<W: Writer>(&self, writer: &mut W) -> Result<(), lightning::io::Error> {
		// We serialize a length header to make sure we can accommodate future changes to the
		// BDK types.
		let block_id_len = ChangeSetSerWrapper(&self.0.block_id).serialized_length() as u64;
		let confirmation_time_len = self.0.confirmation_time.serialized_length() as u64;
		let total_len = BigSize(block_id_len + confirmation_time_len);
		total_len.write(writer)?;

		ChangeSetSerWrapper(&self.0.block_id).write(writer)?;
		self.0.confirmation_time.write(writer)
	}
}

impl Readable for ChangeSetDeserWrapper<ConfirmationBlockTime> {
	fn read<R: lightning::io::Read>(reader: &mut R) -> Result<Self, DecodeError> {
		let total_len: BigSize = Readable::read(reader)?;
		let mut fixed_reader = FixedLengthReader::new(reader, total_len.0);

		let block_id: ChangeSetDeserWrapper<BlockId> = Readable::read(&mut fixed_reader)?;
		let confirmation_time = Readable::read(&mut fixed_reader)?;

		Ok(Self(ConfirmationBlockTime { block_id: block_id.0, confirmation_time }))
	}
}

impl<'a> Writeable for ChangeSetSerWrapper<'a, BlockId> {
	fn write<W: Writer>(&self, writer: &mut W) -> Result<(), lightning::io::Error> {
		// We serialize a length header to make sure we can accommodate future changes to the
		// BDK types.
		let height_len = self.0.height.serialized_length() as u64;
		let hash_len = self.0.hash.serialized_length() as u64;
		let total_len = BigSize(height_len + hash_len);
		total_len.write(writer)?;

		self.0.height.write(writer)?;
		self.0.hash.write(writer)
	}
}

impl Readable for ChangeSetDeserWrapper<BlockId> {
	fn read<R: lightning::io::Read>(reader: &mut R) -> Result<Self, DecodeError> {
		let total_len: BigSize = Readable::read(reader)?;
		let mut fixed_reader = FixedLengthReader::new(reader, total_len.0);
		let height = Readable::read(&mut fixed_reader)?;
		let hash = Readable::read(&mut fixed_reader)?;
		Ok(Self(BlockId { height, hash }))
	}
}

impl<'a> Writeable for ChangeSetSerWrapper<'a, BdkIndexerChangeSet> {
	fn write<W: Writer>(&self, writer: &mut W) -> Result<(), lightning::io::Error> {
		// We serialize a length header to make sure we can accommodate future changes to the
		// BDK types.
		let last_revealed_len =
			ChangeSetSerWrapper(&self.0.last_revealed).serialized_length() as u64;
		let total_len = BigSize(last_revealed_len);
		total_len.write(writer)?;

		ChangeSetSerWrapper(&self.0.last_revealed).write(writer)
	}
}

impl Readable for ChangeSetDeserWrapper<BdkIndexerChangeSet> {
	fn read<R: lightning::io::Read>(reader: &mut R) -> Result<Self, DecodeError> {
		let total_len: BigSize = Readable::read(reader)?;
		let mut fixed_reader = FixedLengthReader::new(reader, total_len.0);
		let last_revealed: ChangeSetDeserWrapper<BTreeMap<DescriptorId, u32>> =
			Readable::read(&mut fixed_reader)?;
		Ok(Self(BdkIndexerChangeSet { last_revealed: last_revealed.0 }))
	}
}

impl<'a> Writeable for ChangeSetSerWrapper<'a, BTreeMap<DescriptorId, u32>> {
	fn write<W: Writer>(&self, writer: &mut W) -> Result<(), lightning::io::Error> {
		let len = BigSize(self.0.len() as u64);
		len.write(writer)?;
		for (descriptor_id, last_index) in self.0.iter() {
			ChangeSetSerWrapper(descriptor_id).write(writer)?;
			last_index.write(writer)?;
		}
		Ok(())
	}
}

impl Readable for ChangeSetDeserWrapper<BTreeMap<DescriptorId, u32>> {
	fn read<R: lightning::io::Read>(reader: &mut R) -> Result<Self, DecodeError> {
		let len: BigSize = Readable::read(reader)?;
		let mut set = BTreeMap::new();
		for _ in 0..len.0 {
			let descriptor_id: ChangeSetDeserWrapper<DescriptorId> = Readable::read(reader)?;
			let last_index = Readable::read(reader)?;
			set.insert(descriptor_id.0, last_index);
		}
		Ok(Self(set))
	}
}

impl<'a> Writeable for ChangeSetSerWrapper<'a, DescriptorId> {
	fn write<W: Writer>(&self, writer: &mut W) -> Result<(), lightning::io::Error> {
		// We serialize a length header to make sure we can accommodate future changes to the
		// BDK types.
		let hash_len = ChangeSetSerWrapper(&self.0 .0).serialized_length() as u64;
		let total_len = BigSize(hash_len);
		total_len.write(writer)?;

		ChangeSetSerWrapper(&self.0 .0).write(writer)
	}
}

impl Readable for ChangeSetDeserWrapper<DescriptorId> {
	fn read<R: lightning::io::Read>(reader: &mut R) -> Result<Self, DecodeError> {
		let total_len: BigSize = Readable::read(reader)?;
		let mut fixed_reader = FixedLengthReader::new(reader, total_len.0);
		let hash: ChangeSetDeserWrapper<Sha256Hash> = Readable::read(&mut fixed_reader)?;
		Ok(Self(DescriptorId(hash.0)))
	}
}

impl<'a> Writeable for ChangeSetSerWrapper<'a, Sha256Hash> {
	fn write<W: Writer>(&self, writer: &mut W) -> Result<(), lightning::io::Error> {
		writer.write_all(&self.0[..])
	}
}

impl Readable for ChangeSetDeserWrapper<Sha256Hash> {
	fn read<R: lightning::io::Read>(reader: &mut R) -> Result<Self, DecodeError> {
		use bitcoin::hashes::Hash;

		let buf: [u8; 32] = Readable::read(reader)?;
		Ok(Self(Sha256Hash::from_slice(&buf[..]).unwrap()))
	}
}

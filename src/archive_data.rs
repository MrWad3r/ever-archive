use std::collections::{BTreeMap, BTreeSet, HashMap};

use ton_block::Deserializable;

use crate::archive_package::*;
use crate::package_entry_id::*;

pub struct ArchiveData<'a> {
    pub mc_block_ids: BTreeMap<u32, ton_block::BlockIdExt>,
    pub blocks: BTreeMap<ton_block::BlockIdExt, ArchiveDataEntry<'a>>,
}

impl<'a> ArchiveData<'a> {
    pub const MAX_MC_BLOCK_COUNT: usize = 100;

    pub fn new(data: &'a [u8]) -> Result<Self, ArchiveDataError> {
        let mut reader = ArchivePackageViewReader::new(data)?;

        let mut res = ArchiveData {
            mc_block_ids: Default::default(),
            blocks: Default::default(),
        };

        while let Some(entry) = reader.read_next()? {
            match PackageEntryId::from_filename(entry.name)? {
                PackageEntryId::Block(id) => {
                    let block = deserialize_block(&id, entry.data)?;

                    res.blocks
                        .entry(id.clone())
                        .or_insert_with(ArchiveDataEntry::default)
                        .block = Some((block, entry.data));
                    if id.shard_id.is_masterchain() {
                        res.mc_block_ids.insert(id.seq_no, id);
                    }
                }
                PackageEntryId::Proof(id) if id.shard_id.is_masterchain() => {
                    let proof = deserialize_block_proof(&id, entry.data, false)?;

                    res.blocks
                        .entry(id.clone())
                        .or_insert_with(ArchiveDataEntry::default)
                        .proof = Some((proof, entry.data));
                    res.mc_block_ids.insert(id.seq_no, id);
                }
                PackageEntryId::ProofLink(id) if !id.shard_id.is_masterchain() => {
                    let proof = deserialize_block_proof(&id, entry.data, true)?;

                    res.blocks
                        .entry(id.clone())
                        .or_insert_with(ArchiveDataEntry::default)
                        .proof = Some((proof, entry.data));
                }
                _ => continue,
            }
        }

        Ok(res)
    }

    pub fn lowest_mc_id(&self) -> Option<&ton_block::BlockIdExt> {
        self.mc_block_ids.values().next()
    }

    pub fn highest_mc_id(&self) -> Option<&ton_block::BlockIdExt> {
        self.mc_block_ids.values().rev().next()
    }

    pub fn check(&self) -> Result<(), ArchiveDataError> {
        let mc_block_count = self.mc_block_ids.len();

        let (left, right) = match (self.lowest_mc_id(), self.highest_mc_id()) {
            (Some(left), Some(right)) => (left.seq_no, right.seq_no),
            _ => return Err(ArchiveDataError::EmptyArchive),
        };

        // NOTE: blocks are stored in BTreeSet so keys are ordered integers
        if (left as usize) + mc_block_count != (right as usize) + 1 {
            return Err(ArchiveDataError::InconsistentMasterchainBlocks);
        }

        // Group all block ids by shards
        let mut map = HashMap::default();
        for block_id in self.blocks.keys() {
            map.entry(block_id.shard_id)
                .or_insert_with(BTreeSet::new)
                .insert(block_id.seq_no);
        }

        // Check consistency
        for (shard_ident, blocks) in &map {
            let mut block_seqnos = blocks.iter();

            // Skip empty shards
            let mut prev = match block_seqnos.next() {
                Some(seqno) => *seqno,
                None => continue,
            };

            // Iterate through all blocks in shard
            for &seqno in block_seqnos {
                // Search either for the previous known block in the same shard
                // or in other shards in case of merge/split
                if seqno != prev + 1 && !contains_previous_block(&map, shard_ident, seqno - 1) {
                    return Err(ArchiveDataError::InconsistentShardchainBlock {
                        shard_ident: *shard_ident,
                        seqno,
                    });
                }
                // Update last known seqno for this shard
                prev = seqno;
            }
        }

        // Archive is not empty and all blocks are contiguous
        Ok(())
    }
}

#[derive(Default)]
pub struct ArchiveDataEntry<'a> {
    pub block: Option<WithData<'a, ton_block::Block>>,
    pub proof: Option<WithData<'a, ton_block::BlockProof>>,
}

impl ArchiveDataEntry<'_> {
    pub fn get_data(
        &self,
    ) -> Result<
        (
            RefWithData<ton_block::Block>,
            RefWithData<ton_block::BlockProof>,
        ),
        ArchiveDataError,
    > {
        let block = match &self.block {
            Some((block, data)) => (block, *data),
            None => return Err(ArchiveDataError::BlockDataNotFound),
        };
        let block_proof = match &self.proof {
            Some((proof, data)) => (proof, *data),
            None => return Err(ArchiveDataError::BlockProofNotFound),
        };
        Ok((block, block_proof))
    }
}

pub fn deserialize_block(
    id: &ton_block::BlockIdExt,
    mut data: &[u8],
) -> Result<ton_block::Block, ArchiveDataError> {
    let file_hash = ton_types::UInt256::calc_file_hash(data);
    if id.file_hash() != file_hash {
        Err(ArchiveDataError::InvalidFileHash)
    } else {
        let root = ton_types::deserialize_tree_of_cells(&mut data)
            .map_err(|_| ArchiveDataError::InvalidBlockData)?;
        if id.root_hash != root.repr_hash() {
            return Err(ArchiveDataError::InvalidRootHash);
        }

        ton_block::Block::construct_from(&mut root.into())
            .map_err(|_| ArchiveDataError::InvalidBlockData)
    }
}

pub fn deserialize_block_proof(
    block_id: &ton_block::BlockIdExt,
    mut data: &[u8],
    is_link: bool,
) -> Result<ton_block::BlockProof, ArchiveDataError> {
    let root = ton_types::deserialize_tree_of_cells(&mut data)
        .map_err(|_| ArchiveDataError::InvalidBlockProof)?;
    let proof = ton_block::BlockProof::construct_from(&mut root.into())
        .map_err(|_| ArchiveDataError::InvalidBlockProof)?;

    if &proof.proof_for != block_id {
        return Err(ArchiveDataError::ProofForAnotherBlock);
    }

    if !block_id.shard_id.is_masterchain() && !is_link {
        Err(ArchiveDataError::ProofForNonMasterchainBlock)
    } else {
        Ok(proof)
    }
}

fn contains_previous_block(
    map: &HashMap<ton_block::ShardIdent, BTreeSet<u32>>,
    shard_ident: &ton_block::ShardIdent,
    prev_seqno: u32,
) -> bool {
    if let Ok((left, right)) = shard_ident.split() {
        // Check case after merge in the same archive in the left child
        if let Some(ids) = map.get(&left) {
            // Search prev seqno in the left shard
            if ids.contains(&prev_seqno) {
                return true;
            }
        }

        // Check case after merge in the same archive in the right child
        if let Some(ids) = map.get(&right) {
            // Search prev seqno in the right shard
            if ids.contains(&prev_seqno) {
                return true;
            }
        }
    }

    if let Ok(parent) = shard_ident.merge() {
        // Check case after second split in the same archive
        if let Some(ids) = map.get(&parent) {
            // Search prev shard in the parent shard
            if ids.contains(&prev_seqno) {
                return true;
            }
        }
    }

    false
}

type WithData<'a, T> = (T, &'a [u8]);
type RefWithData<'a, T> = (&'a T, &'a [u8]);

#[derive(thiserror::Error, Debug)]
pub enum ArchiveDataError {
    #[error("Invalid package")]
    InvalidPackage(#[from] ArchivePackageError),
    #[error("Invalid package entry id")]
    InvalidPackageEntryId(#[from] PackageEntryIdError),
    #[error("Empty archive")]
    EmptyArchive,
    #[error("Inconsistent masterchain blocks")]
    InconsistentMasterchainBlocks,
    #[error("Inconsistent masterchain block {shard_ident}:{seqno}")]
    InconsistentShardchainBlock {
        shard_ident: ton_block::ShardIdent,
        seqno: u32,
    },
    #[error("Block not found in archive")]
    BlockDataNotFound,
    #[error("Block proof not found in archive")]
    BlockProofNotFound,
    #[error("Invalid file hash")]
    InvalidFileHash,
    #[error("Invalid root hash")]
    InvalidRootHash,
    #[error("Invalid block data")]
    InvalidBlockData,
    #[error("Invalid block proof")]
    InvalidBlockProof,
    #[error("Proof for another block")]
    ProofForAnotherBlock,
    #[error("Proof for non-masterchain block")]
    ProofForNonMasterchainBlock,
}

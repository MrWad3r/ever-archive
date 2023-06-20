use std::borrow::Borrow;
use std::hash::Hash;
use std::str::FromStr;
use everscale_types::models as ton_block;

#[derive(Debug, Hash, Eq, PartialEq)]
pub enum PackageEntryId<I> {
    Block(I),
    Proof(I),
    ProofLink(I),
}

impl PackageEntryId<ton_block::BlockId> {
    pub fn from_filename(filename: &str) -> Result<Self, PackageEntryIdError> {
        let block_id_pos = match filename.find('(') {
            Some(pos) => pos,
            None => return Err(PackageEntryIdError::InvalidFileName),
        };

        let (prefix, block_id) = filename.split_at(block_id_pos);

        Ok(match prefix {
            PACKAGE_ENTRY_BLOCK => Self::Block(parse_block_id(block_id)?),
            PACKAGE_ENTRY_PROOF => Self::Proof(parse_block_id(block_id)?),
            PACKAGE_ENTRY_PROOF_LINK => Self::ProofLink(parse_block_id(block_id)?),
            _ => return Err(PackageEntryIdError::InvalidFileName),
        })
    }
}

impl<I> PackageEntryId<I>
    where
        I: Borrow<ton_block::BlockId> + Hash,
{
    fn filename_prefix(&self) -> &'static str {
        match self {
            Self::Block(_) => PACKAGE_ENTRY_BLOCK,
            Self::Proof(_) => PACKAGE_ENTRY_PROOF,
            Self::ProofLink(_) => PACKAGE_ENTRY_PROOF_LINK,
        }
    }
}

pub trait GetFileName {
    fn filename(&self) -> String;
}

impl GetFileName for ton_block::BlockId {
    fn filename(&self) -> String {
        format!(
            "({},{:016x},{}):{}:{}",
            self.shard.workchain(),
            self.shard.prefix(),
            self.seqno,
            hex::encode_upper(self.root_hash.as_slice()),
            hex::encode_upper(self.file_hash.as_slice())
        )
    }
}

impl<I> GetFileName for PackageEntryId<I>
    where
        I: Borrow<ton_block::BlockId> + Hash,
{
    fn filename(&self) -> String {
        match self {
            Self::Block(block_id) | Self::Proof(block_id) | Self::ProofLink(block_id) => {
                format!("{}{}", self.filename_prefix(), block_id.borrow().filename())
            }
        }
    }
}

fn parse_block_id(filename: &str) -> Result<ton_block::BlockId, PackageEntryIdError> {
    let mut parts = filename.split(':');

    let shard_id = match parts.next() {
        Some(part) => part,
        None => return Err(PackageEntryIdError::ShardIdNotFound),
    };

    let mut shard_id_parts = shard_id.split(',');
    let workchain_id = match shard_id_parts
        .next()
        .and_then(|part| part.strip_prefix('('))
    {
        Some(part) => i32::from_str(part).map_err(|_| PackageEntryIdError::InvalidWorkchainId)?,
        None => return Err(PackageEntryIdError::WorkchainIdNotFound),
    };

    let shard_prefix_tagged = match shard_id_parts.next() {
        Some(part) => {
            u64::from_str_radix(part, 16).map_err(|_| PackageEntryIdError::InvalidShardPrefix)?
        }
        None => return Err(PackageEntryIdError::ShardPrefixNotFound),
    };

    let seq_no = match shard_id_parts
        .next()
        .and_then(|part| part.strip_suffix(')'))
    {
        Some(part) => u32::from_str(part).map_err(|_| PackageEntryIdError::InvalidSeqno)?,
        None => return Err(PackageEntryIdError::SeqnoNotFound),
    };

    let shard_id = ton_block::ShardIdent::new(workchain_id, shard_prefix_tagged)
        .ok_or(PackageEntryIdError::InvalidShardPrefix)?;

    let root_hash = match parts.next() {
        Some(part) => {
            hex::decode(part).map_err(|_| PackageEntryIdError::InvalidRootHash)?
        }
        None => return Err(PackageEntryIdError::RootHashNotFound),
    };

    let file_hash = match parts.next() {
        Some(part) => {
            hex::decode(part).map_err(|_| PackageEntryIdError::InvalidFileHash)?
        }
        None => return Err(PackageEntryIdError::FileHashNotFound),
    };

    Ok(ton_block::BlockId {
        shard: shard_id,
        seqno: seq_no,
        root_hash: root_hash.try_into().unwrap(),
        file_hash: file_hash.try_into().unwrap(),
    })
}

#[derive(thiserror::Error, Debug)]
pub enum PackageEntryIdError {
    #[error("Invalid filename")]
    InvalidFileName,
    #[error("Shard id not found")]
    ShardIdNotFound,
    #[error("Workchain id not found")]
    WorkchainIdNotFound,
    #[error("Invalid workchain id")]
    InvalidWorkchainId,
    #[error("Shard prefix not found")]
    ShardPrefixNotFound,
    #[error("Invalid shard prefix")]
    InvalidShardPrefix,
    #[error("Invalid shard ident")]
    InvalidShardIdent,
    #[error("Seqno not found")]
    SeqnoNotFound,
    #[error("Invalid seqno")]
    InvalidSeqno,
    #[error("Root hash not found")]
    RootHashNotFound,
    #[error("Invalid root hash")]
    InvalidRootHash,
    #[error("File hash not found")]
    FileHashNotFound,
    #[error("Invalid file hash")]
    InvalidFileHash,
}

const PACKAGE_ENTRY_BLOCK: &str = "block_";
const PACKAGE_ENTRY_PROOF: &str = "proof_";
const PACKAGE_ENTRY_PROOF_LINK: &str = "prooflink_";

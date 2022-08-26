use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use anyhow::{Context, Result};
use ever_archive::utils::*;
use ever_archive::*;

fn main() {
    if let Err(e) = argh::from_env::<App>().run() {
        eprintln!("{e:?}");
        std::process::exit(1);
    }
}

#[derive(argh::FromArgs)]
#[argh(description = "Everscale block archives viewer")]
struct App {
    #[argh(subcommand)]
    subcommand: Subcommand,
}

impl App {
    fn run(self) -> Result<()> {
        match self.subcommand {
            Subcommand::Check(cmd) => cmd.run(),
            Subcommand::List(cmd) => cmd.run(),
        }
    }
}

struct ListEntry<'a> {
    package_id: Result<PackageEntryId<ton_block::BlockIdExt>, PackageEntryIdError>,
    data: &'a [u8],
    with_size: bool,
}

impl std::fmt::Display for ListEntry<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match &self.package_id {
            Ok(package_id) => f.write_str(&package_id.filename()),
            Err(e) => f.write_fmt(format_args!("<invalid: {e}>")),
        }?;

        if self.with_size {
            f.write_fmt(format_args!(" {}", &self.data.len().to_string()))?;
        }

        Ok(())
    }
}

#[derive(argh::FromArgs)]
#[argh(subcommand)]
enum Subcommand {
    Check(CmdCheck),
    List(CmdList),
}

/// Verifies the archive
#[derive(argh::FromArgs)]
#[argh(subcommand, name = "check")]
struct CmdCheck {
    /// path to the archive file or folder if specified. stdin is used otherwise
    #[argh(option)]
    path: Option<PathBuf>,

    /// shows all key blocks, merges and splits if specified
    #[argh(switch, short = 'a')]
    show_features: bool,
}

impl CmdCheck {
    fn run(self) -> Result<()> {
        match self.path {
            Some(path) if path.is_dir() => {
                let mut files = Vec::new();

                let mut entries = std::fs::read_dir(path)?;
                while let Some(entry) = entries.next() {
                    let path = entry?.path();
                    if path.is_file() {
                        files.push(path);
                    }
                }

                files.sort();

                let pg = indicatif::ProgressBar::new(files.len() as u64);
                for path in files {
                    Self::check_archive(Some(path), self.show_features)?;
                    pg.inc(1);
                }
                Ok(())
            }
            path => Self::check_archive(path, self.show_features),
        }
    }

    fn check_archive(path: Option<PathBuf>, show_features: bool) -> Result<()> {
        use std::collections::hash_map;

        let archive = RawArchive::new(path)?;
        let archive = archive.view()?;

        let archive = ArchiveData::new(archive.as_ref()).context("Failed to parse archive")?;

        struct SimpleList {
            name: &'static str,
            ids: BTreeSet<ton_block::BlockIdExt>,
        }

        impl SimpleList {
            fn new(name: &'static str) -> Self {
                Self {
                    name,
                    ids: Default::default(),
                }
            }

            fn is_empty(&self) -> bool {
                self.ids.is_empty()
            }
        }

        impl std::fmt::Display for SimpleList {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_fmt(format_args!("{}:\n", self.name))?;
                for id in &self.ids {
                    f.write_fmt(format_args!("\t{id}\n"))?;
                }

                Ok(())
            }
        }

        let mut key_blocks = SimpleList::new("Key blocks");
        let mut merges = SimpleList::new("Merges");
        let mut splits = SimpleList::new("Splits");

        fn insert_id_if(
            map: &mut SeqNoMap,
            id: &ton_block::BlockIdExt,
            mut f: impl FnMut(&ton_block::BlockIdExt) -> bool,
        ) {
            if let hash_map::Entry::Occupied(mut entry) = map.entry(id.shard_id) {
                if f(entry.get()) {
                    entry.insert(id.clone());
                }
            }
        }

        type SeqNoMap = HashMap<ton_block::ShardIdent, ton_block::BlockIdExt>;

        let read_shard_blocks = |id: &ton_block::BlockIdExt| -> Result<SeqNoMap> {
            let entry = archive.blocks.get(id).context("Failed to get mc block")?;
            let ((block, _), _) = entry.get_data().context("Missing data for mc block")?;
            let extra = block.read_extra().context("Failed to read block extra")?;
            let custom = extra
                .read_custom()
                .context("Failed to read extra custom")?
                .context("Masterchain block must contain custom")?;

            let mut shards = SeqNoMap::new();
            shards.insert(id.shard_id, id.clone());
            custom.hashes().iterate_shards(|ident, descr| {
                shards.insert(
                    ident,
                    ton_block::BlockIdExt {
                        shard_id: ident,
                        seq_no: descr.seq_no,
                        root_hash: descr.root_hash,
                        file_hash: descr.file_hash,
                    },
                );
                Ok(true)
            })?;

            Ok(shards)
        };

        let mut first_blocks = match archive.mc_block_ids.values().next() {
            Some(first_mc_block) => read_shard_blocks(first_mc_block)
                .with_context(|| format!("Invalid mc block {first_mc_block}"))?,
            None => Default::default(),
        };
        let mut last_blocks = match archive.mc_block_ids.values().next_back() {
            Some(last_mc_block) => read_shard_blocks(last_mc_block)
                .with_context(|| format!("Invalid mc block {last_mc_block}"))?,
            None => Default::default(),
        };

        for (id, entry) in archive.blocks {
            let ((block, _), _) = entry
                .get_data()
                .with_context(|| format!("Missing data for block {id}"))?;

            let info = block
                .read_info()
                .with_context(|| format!("Invalid block data ({id})"))?;

            info.read_master_id()?;

            if info.key_block() {
                key_blocks.ids.insert(id.clone());
            }
            if info.after_merge() {
                merges.ids.insert(id.clone());
            }
            if info.after_split() {
                splits.ids.insert(id.clone());
            }

            insert_id_if(&mut first_blocks, &id, |v| id.seq_no < v.seq_no);
            insert_id_if(&mut last_blocks, &id, |v| id.seq_no > v.seq_no);
        }

        let first_blocks = SimpleList {
            name: "First blocks",
            ids: first_blocks.into_values().collect(),
        };
        let last_blocks = SimpleList {
            name: "Last blocks",
            ids: last_blocks.into_values().collect(),
        };

        if show_features {
            for list in [key_blocks, merges, splits, first_blocks, last_blocks] {
                if !list.is_empty() {
                    print!("{list}");
                }
            }
        }

        Ok(())
    }
}

/// Lists all archive package entries
#[derive(argh::FromArgs)]
#[argh(subcommand, name = "list")]
struct CmdList {
    /// path to the archive if specified. stdin is used otherwise
    #[argh(option)]
    path: Option<PathBuf>,

    /// print entry data size in bytes
    #[argh(switch, short = 's')]
    size: bool,

    /// ignore invalid entries
    #[argh(switch, short = 'i')]
    ignore_invalid: bool,
}

impl CmdList {
    fn run(self) -> Result<()> {
        let archive = RawArchive::new(self.path)?;
        let archive = archive.view()?;

        let mut reader =
            ArchivePackageViewReader::new(archive.as_ref()).context("Invalid archive")?;

        while let Some(entry) = reader.read_next()? {
            let package_id = PackageEntryId::from_filename(entry.name);
            if !self.ignore_invalid && package_id.is_err() {
                return package_id.map(|_| ()).map_err(From::from);
            }

            let item = ListEntry {
                package_id,
                data: entry.data,
                with_size: self.size,
            };
            println!("{item}");
        }

        Ok(())
    }
}

enum RawArchive {
    Bytes(Vec<u8>),
    File(File),
}

impl RawArchive {
    fn new(path: Option<PathBuf>) -> Result<Self> {
        Ok(match path {
            Some(path) => {
                let file = std::fs::OpenOptions::new()
                    .read(true)
                    .open(path)
                    .context("Failed to open archive")?;
                Self::File(file)
            }
            None => {
                let mut buffer = Vec::new();
                std::io::stdin()
                    .read_to_end(&mut buffer)
                    .context("Failed to read archive")?;
                Self::Bytes(buffer)
            }
        })
    }

    fn view(&self) -> std::io::Result<RawArchiveView<'_>> {
        Ok(match self {
            Self::Bytes(bytes) => RawArchiveView::Bytes(bytes),
            Self::File(file) => RawArchiveView::File(FileView::new(file)?),
        })
    }
}

enum RawArchiveView<'a> {
    Bytes(&'a [u8]),
    File(FileView<'a>),
}

impl AsRef<[u8]> for RawArchiveView<'_> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Bytes(bytes) => bytes,
            Self::File(file) => file.as_slice(),
        }
    }
}

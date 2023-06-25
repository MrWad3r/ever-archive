use std::collections::{BTreeSet, HashMap};
use std::ffi::OsStr;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use archive_uploader::{ArchiveUploaderConfig, AwsCredentials};
use everscale_types::models as ton_block;
use indicatif::{ProgressBar, ProgressStyle};
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;
use tokio::sync::{Barrier, Semaphore};

use ever_archive::*;
use ever_archive::utils::*;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;


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
            Subcommand::Upload(cmd) => cmd.run(),
        }
    }
}

struct ListEntry<'a> {
    package_id: Result<PackageEntryId<ton_block::BlockId>, PackageEntryIdError>,
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
    Upload(CmdUpload),
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
                let (files, pg) = init_archive_walker(path);
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
            ids: BTreeSet<ton_block::BlockId>,
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
            id: &ton_block::BlockId,
            mut f: impl FnMut(&ton_block::BlockId) -> bool,
        ) {
            if let hash_map::Entry::Occupied(mut entry) = map.entry(id.shard) {
                if f(entry.get()) {
                    entry.insert(*id);
                }
            }
        }

        type SeqNoMap = HashMap<ton_block::ShardIdent, ton_block::BlockId>;

        let read_shard_blocks = |id: &ton_block::BlockId| -> Result<SeqNoMap> {
            let entry = archive.blocks.get(id).context("Failed to get mc block")?;
            let ((block, _), _) = entry.get_data().context("Missing data for mc block")?;
            let extra = block.load_extra().context("Failed to read block extra")?;
            let custom = extra
                .load_custom()
                .context("Failed to read extra custom")?
                .context("Masterchain block must contain custom")?;

            let mut shards = SeqNoMap::new();
            shards.insert(id.shard, *id);
            custom.shards.iter()
                .filter_map(|x|
                    match x {
                        Ok(t) => { Some((t.0, t.1)) }
                        Err(e) => {
                            eprintln!("Invalid shard description in mc block {id}: {e}");
                            None
                        }
                    })
                .for_each(|(ident, descr)| {
                    shards.insert(
                        ident,
                        ton_block::BlockId {
                            shard: ident,
                            seqno: descr.seqno,
                            root_hash: descr.root_hash,
                            file_hash: descr.file_hash,
                        },
                    );
                });

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
                .load_info()
                .with_context(|| format!("Invalid block data ({id})"))?;


            if info.key_block {
                key_blocks.ids.insert(id);
            }
            if info.after_merge {
                merges.ids.insert(id);
            }
            if info.after_split {
                splits.ids.insert(id);
            }

            insert_id_if(&mut first_blocks, &id, |v| id.seqno < v.seqno);
            insert_id_if(&mut last_blocks, &id, |v| id.seqno > v.seqno);
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

fn init_archive_walker(path: PathBuf) -> (Vec<PathBuf>, ProgressBar) {
    let mut files: Vec<_> = walkdir::WalkDir::new(path).into_iter().filter_map(|x| x.ok())
        .filter(|x| x.file_type().is_file()).map(|x| x.into_path()).
        filter(|x| if let Some(name) = x.file_name() {
            check_filename(name)
        } else { false }
        ).collect();

    files.sort();

    let pg = indicatif::ProgressBar::new(files.len() as u64)
        .with_style(ProgressStyle::with_template("[{elapsed_precise}] {bar:40.cyan/blue} {human_pos}/{human_len} ETA: {eta_precise}. RPS: {per_sec}")
            .unwrap()
            .progress_chars("##-"));
    (files, pg)
}

fn check_filename(os_name: &OsStr) -> bool {
    if let Some(name) = os_name.to_str() {
        let mut name = name.to_string();
        if name.starts_with("0") {
            name.replace_range(0..1, "");
        }
        if let Ok(seqno) = name.parse::<u64>() {
            if seqno >= 11650126u64 && seqno <= 12983255u64 {
                println!("Found suitable archive: {seqno}");
                return true;
            }
        }
    }
    println!("Bad archive found: {os_name:?}");
    false
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

#[derive(argh::FromArgs)]
#[argh(subcommand, name = "upload")]
/// Uploads archive to the cloud storage
struct CmdUpload {
    #[argh(option, short = 'p')]
    /// path to the archive root directory
    path: PathBuf,

    /// name of the endpoint (e.g. `"eu-east-2"`)
    #[argh(option)]
    pub name: String,

    /// endpoint to be used. For instance, `"https://s3.my-provider.net"` or just
    /// `"s3.my-provider.net"` (default scheme is https).
    #[argh(option)]
    pub endpoint: String,

    /// bucket name
    #[argh(option)]
    pub bucket: String,

    /// aws access key ID
    #[argh(option)]
    pub access_key: String,

    /// aws secret access key
    #[argh(option)]
    pub secret_key: String,
}

impl CmdUpload {
    fn run(self) -> Result<()> {
        let runner = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .context("Failed to create tokio runtime")?;

        let creds = AwsCredentials {
            access_key: self.access_key,
            secret_key: self.secret_key,
            token: None,
        };
        let config = ArchiveUploaderConfig {
            name: self.name,
            endpoint: self.endpoint,
            bucket: self.bucket,
            archive_key_prefix: "".to_string(),
            archives_search_interval_sec: 600,
            retry_interval_ms: 100,
            credentials: Some(creds),

        };
        let s3_client = runner.block_on(archive_uploader::ArchiveUploader::new(config)).context("Failed to create s3 client")?;

        let (files, pg) = init_archive_walker(self.path);
        let semaphore = Arc::new(Semaphore::new(4));
        let barier = Arc::new(Barrier::new(files.len() + 1));

        for file in files {
            let semaphore = semaphore.clone();
            let s3_client = s3_client.clone();
            let pg = pg.clone();
            let barier = barier.clone();

            runner.spawn(async move {
                let permit = semaphore.acquire().await.unwrap();
                let data = match tokio::fs::read(&file).await {
                    Ok(data) => data,
                    Err(e) => {
                        eprintln!("Failed to read file {}: {}", file.display(), e);
                        return;
                    }
                };
                let archive = match ArchiveData::new(&data) {
                    Ok(archive) => archive,
                    Err(e) => {
                        eprintln!("Failed to parse archive {}: {}", file.display(), e);
                        return;
                    }
                };
                if let Err(e) = archive.check() {
                    eprintln!("Failed to check archive {}: {}", file.display(), e);
                    return;
                }
                let lowest_id = match archive.lowest_mc_id() {
                    Some(id) => id.seqno,
                    None => {
                        eprintln!("Archive {} is empty", file.display());
                        return;
                    }
                };
                drop(archive);

                s3_client.upload(lowest_id, data).await;

                pg.inc(1);
                drop(permit);

                barier.wait().await;
            });
        }
        runner.block_on(barier.wait());

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

use std::path::PathBuf;

use clap::Parser;

use katniss_ingestor::ingestors::proto_repeated::{RepeatedProtoIngestor, Serialization};
use katniss_pb2arrow::ArrowBatchProps;
use katniss_test::descriptor_pool;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let cli = Cli::parse();
    let source = cli.source();

    tracing::info!(
        "starting ingrest of {:#?} to {:#?}",
        cli.source(),
        cli.destination()
    );
    tracing::info!("reading {source:#?} into memory");
    let source_file = std::fs::read(cli.source())?;
    tracing::info!("read {source:#?} into memory");

    let ingestor = RepeatedProtoIngestor::new(
        &source_file[..],
        ArrowBatchProps::new(descriptor_pool()?, "Packet".to_owned(), 1024)?,
        cli.serialization(),
    )?;

    tracing::info!("Ingesting...");
    ingestor.ingest()?;
    tracing::info!("Woooohoooo!");

    Ok(())
}

#[derive(Parser, Debug)]
struct Cli {
    #[arg(short, long)]
    source: PathBuf,
    #[arg(short, long, value_enum)]
    format: Format,
    #[arg(short, long)]
    dest: Option<PathBuf>,
}

#[derive(clap::ValueEnum, Clone, Debug)]
enum Format {
    Parquet,
    Lance,
}

impl Cli {
    fn source(&self) -> PathBuf {
        self.source.clone()
    }

    fn destination(&self) -> PathBuf {
        self.dest
            .clone()
            .unwrap_or_else(|| self.source.clone())
            .as_path()
            .with_extension(self.extension())
    }

    fn extension(&self) -> &'static str {
        match self.format {
            Format::Parquet => "parquet",
            Format::Lance => "lance",
        }
    }

    fn serialization(&self) -> Serialization<PathBuf> {
        match self.format {
            Format::Parquet => Serialization::Parquet {
                filename: self.destination(),
            },
            Format::Lance => Serialization::Lance {
                filename: self.destination(),
            },
        }
    }
}

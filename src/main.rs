use clap::{Parser, Subcommand};

mod kinesis;

use kinesis::KinesisCommand;

#[derive(Parser)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    #[clap(alias = "ki")]
    Kinesis(KinesisCommand),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();
    match cli.command {
        Commands::Kinesis(subcommand) => subcommand.exec().await?,
    }
    Ok(())
}

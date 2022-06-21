use std::time::Duration;

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_kinesis::model::{PutRecordsRequestEntry, ShardIteratorType};
use aws_sdk_kinesis::types::Blob;
use aws_sdk_kinesis::{Client, Region};
use clap::{Args, Subcommand};
use tokio::io::AsyncBufReadExt;

const ONE_MIB: usize = 1024 * 1024;

#[derive(Args)]
pub struct KinesisCommand {
    #[clap(global = true, long)]
    region: Option<String>,
    #[clap(subcommand)]
    subcommand: KinesisSubcommands,
}

#[derive(Subcommand)]
pub enum KinesisSubcommands {
    #[clap(alias = "mk")]
    Create {
        #[clap(long)]
        stream_name: String,
        #[clap(long, default_value_t = 1)]
        num_shards: usize,
    },
    #[clap(alias = "rm")]
    Delete {
        #[clap(long)]
        stream_name: String,
    },
    #[clap(alias = "ls")]
    List,
    #[clap(alias = "lss")]
    ListShards {
        #[clap(long)]
        stream_name: String,
    },
    Push {
        #[clap(long)]
        stream_name: String,
    },
    ScaleUp {
        #[clap(long)]
        stream_name: String,
    },
    ScaleDown {
        #[clap(long)]
        stream_name: String,
    },
    Tail {
        #[clap(long)]
        stream_name: String,
        #[clap(long)]
        shard_id: usize,
    },
}

async fn create_stream(
    client: &Client,
    stream_name: &str,
    num_shards: usize,
) -> anyhow::Result<()> {
    client
        .create_stream()
        .stream_name(stream_name)
        .shard_count(num_shards as i32)
        .send()
        .await?;
    println!(
        "Created stream `{}` with {} shard(s).",
        stream_name, num_shards
    );
    Ok(())
}

async fn delete_stream(client: &Client, stream_name: &str) -> anyhow::Result<()> {
    client
        .delete_stream()
        .stream_name(stream_name)
        .send()
        .await?;
    println!("Deleted stream `{}` successfully.", stream_name);
    Ok(())
}

async fn list_streams(client: &Client) -> anyhow::Result<()> {
    let output = client.list_streams().send().await?;

    // TODO: handle `has_more_stream_names`.
    if let Some(stream_names) = output.stream_names {
        for stream_name in stream_names {
            println!("{}", stream_name);
        }
    }
    Ok(())
}

async fn list_shards(client: &Client, stream_name: &str) -> anyhow::Result<()> {
    let output = client.list_shards().stream_name(stream_name).send().await?;

    // TODO: handle `next_token`.
    if let Some(shards) = output.shards {
        for shard in shards {
            println!("{}", shard.shard_id.unwrap());
        }
    }
    Ok(())
}

async fn tail(client: &Client, stream_name: &str, shard_id: usize) -> anyhow::Result<()> {
    let mut shard_iterator_opt = client
        .get_shard_iterator()
        .stream_name(stream_name)
        .shard_id(make_shard_id(shard_id))
        .shard_iterator_type(ShardIteratorType::Latest)
        .send()
        .await?
        .shard_iterator;

    let mut interval = tokio::time::interval(Duration::from_millis(205));

    while let Some(shard_iterator) = shard_iterator_opt {
        interval.tick().await;

        let output = client
            .get_records()
            .shard_iterator(shard_iterator)
            .send()
            .await?;

        if let Some(records) = output.records {
            for record in records {
                let line = record
                    .data()
                    .map(|blob| std::str::from_utf8(blob.as_ref()))
                    .transpose()?
                    .unwrap_or("Record payload is empty.");
                println!("{}", line);
            }
        }
        shard_iterator_opt = output.next_shard_iterator;
    }
    Ok(())
}

async fn put_records(
    client: &Client,
    stream_name: &str,
    records: Vec<PutRecordsRequestEntry>,
) -> anyhow::Result<()> {
    client
        .put_records()
        .set_records(Some(records))
        .stream_name(stream_name)
        .send()
        .await?;
    Ok(())
}

async fn push(client: &Client, stream_name: &str) -> anyhow::Result<()> {
    let stdin = tokio::io::stdin();
    let reader = tokio::io::BufReader::new(stdin);
    let mut lines = reader.lines();

    let mut num_bytes = 0;
    let mut num_records = 0;
    let mut records = Vec::new();

    while let Some(line) = lines.next_line().await? {
        let record_len = line.len();
        num_records += 1;

        if record_len > ONE_MIB {
            println!("Record #{} is larger than 1 MiB, skipping.", num_records);
            continue;
        }
        if num_bytes + record_len > 5 * ONE_MIB {
            put_records(
                client,
                stream_name,
                std::mem::replace(&mut records, Vec::new()),
            )
            .await?;
        }
        num_bytes += record_len;

        let record = PutRecordsRequestEntry::builder()
            .partition_key(format!("{:x}", seahash::hash(line.as_bytes())))
            .data(Blob::new(line))
            .build();
        records.push(record);

        if records.len() == 500 {
            put_records(
                client,
                stream_name,
                std::mem::replace(&mut records, Vec::new()),
            )
            .await?;
            num_bytes = 0;
        }
    }
    if records.len() > 0 {
        put_records(client, stream_name, records).await?;
    }
    println!(
        "Pushed {} records to stream `{}` in {}s ({} MiB/s).",
        num_records, stream_name, "0", "0"
    );
    Ok(())
}

async fn scale_up(client: &Client, stream_name: &str) -> anyhow::Result<()> {
    // client
    //     .split_shard()
    //     .stream_name(stream_name)
    Ok(())
}

async fn scale_down(client: &Client, stream_name: &str) -> anyhow::Result<()> {
    // client
    //     .merge_shards()
    //     .stream_name(stream_name)
    Ok(())
}

fn make_shard_id(id: usize) -> String {
    format!("shardId-{:0>12}", id)
}

impl KinesisCommand {
    pub async fn exec(self) -> anyhow::Result<()> {
        let region_provider = RegionProviderChain::first_try(self.region.map(Region::new))
            .or_default_provider()
            .or_else(Region::new("us-east-1"));
        let config = aws_config::from_env().region(region_provider).load().await;
        let client = Client::new(&config);

        match self.subcommand {
            KinesisSubcommands::Create {
                stream_name,
                num_shards,
            } => create_stream(&client, &stream_name, num_shards).await?,
            KinesisSubcommands::Delete { stream_name } => {
                delete_stream(&client, &stream_name).await?
            }
            KinesisSubcommands::List => list_streams(&client).await?,
            KinesisSubcommands::ListShards { stream_name } => {
                list_shards(&client, &stream_name).await?
            }
            KinesisSubcommands::Push { stream_name } => push(&client, &stream_name).await?,
            KinesisSubcommands::ScaleUp { stream_name } => scale_up(&client, &stream_name).await?,
            KinesisSubcommands::ScaleDown { stream_name } => {
                scale_down(&client, &stream_name).await?
            }
            KinesisSubcommands::Tail {
                stream_name,
                shard_id,
            } => tail(&client, &stream_name, shard_id).await?,
        };
        Ok(())
    }
}

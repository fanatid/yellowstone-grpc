use {
    clap::Parser,
    futures::stream::StreamExt,
    indicatif::{ProgressBar, ProgressStyle},
    maplit::hashmap,
    std::{collections::HashMap, env},
    tonic::transport::channel::ClientTlsConfig,
    yellowstone_grpc_client::GeyserGrpcClient,
    yellowstone_grpc_proto::prelude::{
        CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts,
        SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterEntry, SubscribeRequestFilterSlots,
        SubscribeRequestFilterTransactions,
    },
};

#[derive(Debug, Clone, Parser)]
#[clap(author, version, about)]
struct Args {
    #[clap(short, long, default_value_t = String::from("http://127.0.0.1:10000"))]
    /// Service endpoint
    endpoint: String,

    #[clap(long)]
    x_token: Option<String>,

    #[clap(long)]
    threshold: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env::set_var(
        env_logger::DEFAULT_FILTER_ENV,
        env::var_os(env_logger::DEFAULT_FILTER_ENV).unwrap_or_else(|| "info".into()),
    );
    env_logger::init();

    let args = Args::parse();

    let request = SubscribeRequest {
        accounts: hashmap! { "".to_owned() => SubscribeRequestFilterAccounts::default() },
        slots: hashmap! { "".to_owned() => SubscribeRequestFilterSlots {
            filter_by_commitment: Some(false),
            interslot_updates: Some(true),
        } },
        transactions: hashmap! { "".to_owned() => SubscribeRequestFilterTransactions::default() },
        transactions_status: HashMap::new(),
        blocks: HashMap::new(),
        blocks_meta: hashmap! { "".to_owned() => SubscribeRequestFilterBlocksMeta::default() },
        entry: hashmap! { "".to_owned() => SubscribeRequestFilterEntry::default() },
        commitment: Some(CommitmentLevel::Processed as i32),
        accounts_data_slice: vec![],
        ping: None,
        from_slot: None,
    };

    let mut client = GeyserGrpcClient::build_from_shared(args.endpoint.clone())?
        .x_token(args.x_token.clone())?
        .tls_config(ClientTlsConfig::new().with_native_roots())?
        .max_decoding_message_size(16 * 1024 * 1024)
        .connect()
        .await?;
    let (_subscribe_tx, mut stream) = client.subscribe_with_request(Some(request)).await?;

    let pb = ProgressBar::no_length();
    pb.set_style(ProgressStyle::with_template(&format!(
        "{{spinner}} total: {{pos}} split: {{msg}}"
    ))?);

    let (mut count_before, mut count_after) = (0, 0);
    while let Some(message) = stream.next().await {
        let Some(update) = message?.update_oneof else {
            anyhow::bail!("failed to get update_oneof");
        };
        let size = update.encoded_len();
        if size > args.threshold {
            count_after += 1;
        } else {
            count_before += 1;
        }
        pb.set_message(format!(
            "{count_before} <= {} < {count_after} ({:.2?}%)",
            args.threshold,
            100.0 * count_before as f64 / (count_before + count_after) as f64
        ));
        pb.inc(1);
    }

    Ok(())
}

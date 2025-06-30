use {
    clap::Parser,
    log::{error, info},
    std::{env, net::SocketAddr},
    tokio::{sync::mpsc, time::Duration},
    tokio_stream::wrappers::ReceiverStream,
    tonic::{
        codec::CompressionEncoding,
        transport::server::{Server, TcpIncoming},
        Request, Response, Result as TonicResult, Status, Streaming,
    },
    yellowstone_grpc_proto::{
        plugin::{
            filter::message::FilteredUpdate,
            proto::geyser_server::{Geyser, GeyserServer},
        },
        prelude::{
            GetBlockHeightRequest, GetBlockHeightResponse, GetLatestBlockhashRequest,
            GetLatestBlockhashResponse, GetSlotRequest, GetSlotResponse, GetVersionRequest,
            GetVersionResponse, IsBlockhashValidRequest, IsBlockhashValidResponse, PingRequest,
            PongResponse, SubscribeReplayInfoRequest, SubscribeReplayInfoResponse,
            SubscribeRequest,
        },
    },
};

#[derive(Debug, Clone, Parser)]
#[clap(author, version, about)]
struct Args {
    #[clap(long)]
    bind: SocketAddr,
}

struct GrpcService;

#[tonic::async_trait]
impl Geyser for GrpcService {
    type SubscribeStream = ReceiverStream<TonicResult<FilteredUpdate>>;

    async fn subscribe(
        &self,
        mut request: Request<Streaming<SubscribeRequest>>,
    ) -> TonicResult<Response<Self::SubscribeStream>> {
        let (tx, rx) = mpsc::channel(1);

        tokio::spawn(async move {
            loop {
                let Ok(Some(mut request)) = request.get_mut().message().await else {
                    error!("failed to get new subscribe message");
                    break;
                };

                for item in request.accounts.values_mut() {
                    item.account.truncate(1);
                    item.owner.truncate(1);
                }
                for item in request.transactions.values_mut() {
                    item.account_include.truncate(1);
                    item.account_exclude.truncate(1);
                    item.account_required.truncate(1);
                }

                info!("{request:?}");
            }

            drop(tx);
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn subscribe_first_available_slot(
        &self,
        _request: Request<SubscribeReplayInfoRequest>,
    ) -> Result<Response<SubscribeReplayInfoResponse>, Status> {
        todo!()
    }

    async fn ping(&self, _request: Request<PingRequest>) -> Result<Response<PongResponse>, Status> {
        todo!()
    }

    async fn get_latest_blockhash(
        &self,
        _request: Request<GetLatestBlockhashRequest>,
    ) -> Result<Response<GetLatestBlockhashResponse>, Status> {
        todo!()
    }

    async fn get_block_height(
        &self,
        _request: Request<GetBlockHeightRequest>,
    ) -> Result<Response<GetBlockHeightResponse>, Status> {
        todo!()
    }

    async fn get_slot(
        &self,
        _request: Request<GetSlotRequest>,
    ) -> Result<Response<GetSlotResponse>, Status> {
        todo!()
    }

    async fn is_blockhash_valid(
        &self,
        _request: Request<IsBlockhashValidRequest>,
    ) -> Result<Response<IsBlockhashValidResponse>, Status> {
        todo!()
    }

    async fn get_version(
        &self,
        _request: Request<GetVersionRequest>,
    ) -> Result<Response<GetVersionResponse>, Status> {
        todo!()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env::set_var(
        env_logger::DEFAULT_FILTER_ENV,
        env::var_os(env_logger::DEFAULT_FILTER_ENV).unwrap_or_else(|| "info".into()),
    );
    env_logger::init();

    let args = Args::parse();

    let incoming = TcpIncoming::new(
        args.bind,
        true,                          // tcp_nodelay
        Some(Duration::from_secs(20)), // tcp_keepalive
    )
    .map_err(|error| anyhow::anyhow!(error))?;
    info!("binded to {:?}", args.bind);

    let service = GeyserServer::new(GrpcService)
        .max_decoding_message_size(16 * 1024 * 1024)
        .accept_compressed(CompressionEncoding::Gzip)
        .send_compressed(CompressionEncoding::Gzip);

    Server::builder()
        .add_service(service)
        .serve_with_incoming(incoming)
        .await
        .map_err(Into::into)
}

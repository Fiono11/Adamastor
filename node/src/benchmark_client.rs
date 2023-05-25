// Copyright(C) Facebook, Inc. and its affiliates.
use anyhow::{Context, Result};
use bytes::BufMut as _;
use bytes::BytesMut;
use clap::{crate_name, crate_version, App, AppSettings};
use curve25519_dalek::scalar::Scalar;
use env_logger::Env;
use futures::future::join_all;
use futures::sink::SinkExt as _;
use log::{info, warn};
use mc_account_keys::AccountKey;
use mc_account_keys::PublicAddress;
use mc_crypto_keys::RistrettoPrivate;
use mc_transaction_core::tx::TxOut;
use mc_transaction_core::tx::create_transaction;
use mc_transaction_types::Amount;
use rand::Rng;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::time::{interval, sleep, Duration, Instant};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use bytes::Bytes;

#[tokio::main]
async fn main() -> Result<()> {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about("Benchmark client for Narwhal and Tusk.")
        .args_from_usage("<ADDR> 'The network address of the node where to send txs'")
        .args_from_usage("--size=<INT> 'The size of each transaction in bytes'")
        .args_from_usage("--rate=<INT> 'The rate (txs/s) at which to send the transactions'")
        .args_from_usage("--nodes=[ADDR]... 'Network addresses that must be reachable before starting the benchmark.'")
        .setting(AppSettings::ArgRequiredElseHelp)
        .get_matches();

    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let target = matches
        .value_of("ADDR")
        .unwrap()
        .parse::<SocketAddr>()
        .context("Invalid socket address format")?;
    let size = matches
        .value_of("size")
        .unwrap()
        .parse::<usize>()
        .context("The size of transactions must be a non-negative integer")?;
    let rate = matches
        .value_of("rate")
        .unwrap()
        .parse::<u64>()
        .context("The rate of transactions must be a non-negative integer")?;
    let nodes = matches
        .values_of("nodes")
        .unwrap_or_default()
        .into_iter()
        .map(|x| x.parse::<SocketAddr>())
        .collect::<Result<Vec<_>, _>>()
        .context("Invalid socket address format")?;

    info!("Node address: {}", target);

    // NOTE: This log entry is used to compute performance.
    info!("Transactions size: {} B", size);

    // NOTE: This log entry is used to compute performance.
    info!("Transactions rate: {} tx/s", rate);

    let client = Client {
        target,
        size,
        rate,
        nodes,
    };

    // Wait for all nodes to be online and synchronized.
    client.wait().await;

    // Start the benchmark.
    client.send().await.context("Failed to submit transactions")
}

struct Client {
    target: SocketAddr,
    size: usize,
    rate: u64,
    nodes: Vec<SocketAddr>,
}

impl Client {
    pub async fn send(&self) -> Result<()> {
        const PRECISION: u64 = 20; // Sample precision.
        const BURST_DURATION: u64 = 1000 / PRECISION;

        // The transaction size must be at least 16 bytes to ensure all txs are different.
        if self.size < 9 {
            return Err(anyhow::Error::msg(
                "Transaction size must be at least 9 bytes",
            ));
        }

        // Connect to the mempool.
        let stream = TcpStream::connect(self.target)
            .await
            .context(format!("failed to connect to {}", self.target))?;

        // Submit all transactions.
        let burst = self.rate / PRECISION;
        let size = 9;
        let mut counter = 0;
        let mut r = rand::thread_rng().gen();
        let mut rng = rand_core::OsRng;
        let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
        let interval = interval(Duration::from_millis(BURST_DURATION));
        tokio::pin!(interval);

        // NOTE: This log entry is used to compute performance.
        info!("Start sending transactions");

        let amount = Amount::new(1);
        let recipient = PublicAddress::default();
        let tx_private_key = RistrettoPrivate::default();
        let sender = AccountKey::default();
        let coin_key = Scalar::random(&mut rng);
        let tx_out = TxOut::new(amount, &recipient, &tx_private_key, sender.to_public_address(), coin_key).unwrap(); 
        let mut tx = create_transaction(&tx_out, &sender, &recipient, amount.value, Vec::new());
        let mut id = BytesMut::with_capacity(size);
            
        'main: loop {
            interval.as_mut().tick().await;
            let now = Instant::now();
    
                for x in 0..burst {
                    if x == counter % burst {
                        // NOTE: This log entry is used to compute performance.
                        info!("Sending sample transaction {}", counter);
                        id.put_u8(0u8); // Sample txs start with 0.
                        id.put_u64(counter); // This counter identifies the tx.
                    } else {
                        r += 1;
                        id.put_u8(1u8); // Standard txs start with 1.
                        id.put_u64(r); // Ensures all clients send different txs.
                    };
    
                    tx.id = id.to_vec();
                    let message = bincode::serialize(&tx.clone()).unwrap();
                    id.resize(size, 0u8);
                    //id.split();

                    //info!("Sending transaction {}", tx.tx_hash());
                    let bytes = Bytes::from(message);
                    if let Err(e) = transport.send(bytes).await {
                        warn!("Failed to send transaction: {}", e);
                        break 'main;
                    }
                }
                if now.elapsed().as_millis() > BURST_DURATION as u128 {
                    // NOTE: This log entry is used to compute performance.
                    warn!("Transaction rate too high for this client");
                }
                counter += 1;
    
        }
        Ok(())
    }

    pub async fn wait(&self) {
        // Wait for all nodes to be online.
        info!("Waiting for all nodes to be online...");
        join_all(self.nodes.iter().rev().cloned().map(|address| {
            tokio::spawn(async move {
                while TcpStream::connect(address).await.is_err() {
                    sleep(Duration::from_millis(100)).await;
                    info!("Connecting to {:?}", address);
                }
            })
        }))
        .await;
    }
}


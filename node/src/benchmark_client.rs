// Copyright(C) Facebook, Inc. and its affiliates.
use anyhow::{Context, Result};
use bytes::BufMut as _;
use bytes::Bytes;
use bytes::BytesMut;
use clap::{crate_name, crate_version, App, AppSettings};
use env_logger::Env;
use futures::future::join_all;
use futures::sink::SinkExt as _;
use log::{info, warn};
use primary::Transaction;
use rand::thread_rng;
use rand::Rng;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::time::{sleep, Duration, Instant, interval};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[tokio::main]
async fn main() -> Result<()> {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about("Benchmark client for Narwhal and Tusk.")
        .args_from_usage("<ADDR> 'The network address of the node where to send txs'")
        .args_from_usage("--size=<INT> 'The size of each transaction in bytes'")
        .args_from_usage("--rate=<INT> 'The rate (txs/s) at which to send the transactions'")
        .args_from_usage("--nodes=[ADDR]... 'Network addresses that must be reachable before starting the benchmark.'")
        .arg_from_usage("--id=<INT> 'The id of the node")
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
    let id: u64 = matches
        .value_of("id")
        .unwrap()
        .parse::<u64>()
        .context("The id of the node")?;

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
        id,
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
    id: u64,
}

impl Client {
    pub async fn send(&self) -> Result<()> {
        let mut counter2 = 0;

        //if self.id < (self.nodes.len() as u64) - (self.nodes.len() as u64 - 1) / 3 {
            const PRECISION: u64 = 20; // Sample precision.
            const BURST_DURATION: u64 = 1000 / PRECISION;

            // The transaction size must be at least 16 bytes to ensure all txs are different.
            if self.size < 9 {
                return Err(anyhow::Error::msg(
                    "Transaction size must be at least 9 bytes",
                ));
            }

            let size = 13;

            // Connect to the mempool.
            let stream = TcpStream::connect(self.target)
                .await
                .context(format!("failed to connect to {}", self.target))?;

            // Submit all transactions.
            let burst = self.rate / PRECISION;
            //let burst = 20;
            let mut data: Vec<u8> = Vec::new();
            for _ in 0..(self.size - 32) {
                data.push(rand::thread_rng().gen());
                //data.push(0);
            }
            let mut id: BytesMut = BytesMut::with_capacity(size);
            let mut tx = Transaction::new();
            tx.data = data;
            let mut counter = 0;
            let mut r: u64 = thread_rng().gen();
            let mut r2: u32 = thread_rng().gen();
            //let mut r: u64 = 0;
            let mut forks = false;
            if r == 0 {
                forks = true;
            }
            info!("Forks: {}", forks);
            let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
            let interval = interval(Duration::from_millis(BURST_DURATION));
            tokio::pin!(interval);

            // NOTE: This log entry is used to compute performance.
            info!(
                "Start sending {} transactions",
                PRECISION * burst * (self.nodes.len() as u64)
            );

            info!("RATE: {}", self.rate);

            'main: loop {
            //for _ in 0..self.rate {//PRECISION * (self.nodes.len() as u64) {
                interval.as_mut().tick().await;
                let now = Instant::now();

                for x in 0..burst {
                    //if x == counter % burst {
                        //r += 1;
                        //id.put_u8(0u8); // Sample txs start with 0.
                                        //id.put_u64(r);
                        //id.put_u64(counter); // This counter identifies the tx.
                                             //id.put_u32(r2);

                    // NOTE: This log entry is used to compute performance.
                    //info!("Sending sample transaction {}", counter);
                    //} else {
                        r += 1;
                        id.put_u8(1u8); // Standard txs start with 1.
                        id.put_u64(r); // Ensures all clients send different txs.
                    //};

                    tx.id = id.to_vec();
                    //if self.id != 0 {
                        info!(
                            "Sending sample transaction {}",
                            //counter2
                            self.rate * self.id + counter2
                        );

                        //info!("counter: {}", counter2);
                    //}
                    //info!("Sending transaction with id {:?} and digest {:?}", tx.id, tx.digest());
                    let message = bincode::serialize(&tx.clone()).unwrap();
                    //if counter == 0 {
                    //info!("TX SIZE: {:?}", message.len());
                    //}
                    id.resize(size, 0u8);
                    id.split();

                    let bytes = Bytes::from(message);

                    if let Err(e) = transport.send(bytes.clone()).await {
                        warn!("Failed to send transaction: {}", e);
                        break 'main;
                    }
                    counter2 += 1;
                }
                if now.elapsed().as_millis() > BURST_DURATION as u128 {
                    // NOTE: This log entry is used to compute performance.
                    warn!("Transaction rate too high for this client");
                }
                counter += 1;
            }
            info!("Sent {} txs", counter2);
            if forks {
                info!("Total bytes: {}", counter2 * 532);
            } else {
                info!(
                    "Total bytes: {}",
                    counter2 * 532 * (self.nodes.len() - (self.nodes.len() - 1) / 3) as u64
                );
            }
        //}
        /*else {
            info!(
                "Start sending transactions",
            );
            for _ in 0..self.rate {
                info!(
                    "Sending sample transaction {}",
                    counter2
                );
                counter2 += 1;
            }
        }*/
        Ok(())
    }

    pub async fn wait(&self) {
        // Wait for all nodes to be online.
        info!("Waiting for all nodes to be online...");
        join_all(self.nodes.iter().cloned().map(|address| {
            tokio::spawn(async move {
                while TcpStream::connect(address).await.is_err() {
                    sleep(Duration::from_millis(10)).await;
                }
            })
        }))
        .await;
    }
}
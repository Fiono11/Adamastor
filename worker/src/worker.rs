// Copyright(C) Facebook, Inc. and its affiliates.
use crate::batch_maker::{Batch, BatchMaker};
use crate::helper::Helper;
use crate::primary_connector::PrimaryConnector;
use crate::processor::{Processor, SerializedBatchMessage};
use crate::quorum_waiter::QuorumWaiter;
use crate::synchronizer::Synchronizer;
use async_trait::async_trait;
use bulletproofs_og::{PedersenGens, RangeProof};
use bytes::Bytes;
use chacha20poly1305::aead::{Aead, OsRng};
use chacha20poly1305::{Key, ChaCha20Poly1305, KeyInit, Nonce};
use config::{Committee, Parameters, WorkerId, PK};
use curve25519_dalek::constants::RISTRETTO_BASEPOINT_POINT;
use curve25519_dalek::ristretto::CompressedRistretto;
use curve25519_dalek::traits::Identity;
use mc_account_keys::{PublicAddress as PublicKey, AccountKey};
use mc_crypto_keys::{RistrettoPublic, RistrettoPrivate, ReprBytes, GenericArray};
use mc_crypto_keys::tx_hash::TxHash as Digest;
use futures::sink::SinkExt as _;
use log::{error, info, warn, debug};
use mc_crypto_ring_signature::onetime_keys::{create_shared_secret, recover_onetime_private_key};
use mc_crypto_ring_signature::{Verify, KeyGen, RistrettoPoint, Scalar};
use mc_transaction_core::range_proofs::check_range_proof;
use mc_transaction_types::constants::RING_SIZE;
use network::{MessageHandler, Receiver, Writer};
use primary::PrimaryWorkerMessage;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::net::SocketAddr;
use std::time::{Instant, Duration};
use store::Store;
use tokio::sync::mpsc::{channel, Sender};
use mc_transaction_core::tx::{Transaction, get_subaddress};

#[cfg(test)]
#[path = "tests/worker_tests.rs"]
pub mod worker_tests;

/// The default channel capacity for each channel of the worker.
pub const CHANNEL_CAPACITY: usize = 100_0000;

/// The primary round number.
// TODO: Move to the primary.
pub type Round = u64;

/// Indicates a serialized `WorkerPrimaryMessage` message.
pub type SerializedBatchDigestMessage = Vec<u8>;

/// The message exchanged between workers.
#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerMessage {
    Batch(Block),
    BatchRequest(Vec<Digest>, /* origin */ PublicKey),
}

pub struct Worker {
    /// The public key of this authority.
    name: PublicKey,
    /// The id of this worker.
    id: WorkerId,
    /// The committee information.
    committee: Committee,
    /// The configuration parameters.
    parameters: Parameters,
    /// The persistent storage.
    store: Store,
}

impl Worker {
    pub fn spawn(
        name: PublicKey,
        id: WorkerId,
        committee: Committee,
        parameters: Parameters,
        store: Store,
    ) {
        // Define a worker instance.
        let worker = Self {
            name,
            id,
            committee,
            parameters,
            store,
        };

        let primary_address = worker
            .committee
            .primary(&PK(worker.name.to_bytes()))
            .expect("Our public key is not in the committee")
            .worker_to_primary;

        // Spawn all worker tasks.
        let (tx_primary, rx_primary) = channel(CHANNEL_CAPACITY);
        worker.handle_primary_messages();
        worker.handle_clients_transactions(tx_primary.clone(), primary_address);
        worker.handle_workers_messages(tx_primary);

        // The `PrimaryConnector` allows the worker to send messages to its primary.
        PrimaryConnector::spawn(
            worker
                .committee
                .primary(&PK(worker.name.to_bytes()))
                .expect("Our public key is not in the committee")
                .worker_to_primary,
            rx_primary,
        );

        // NOTE: This log entry is used to compute performance.
        info!(
            "Worker {} successfully booted on {}",
            id,
            worker
                .committee
                .worker(&PK(worker.name.to_bytes()), &worker.id)
                .expect("Our public key or worker id is not in the committee")
                .transactions
                .ip()
        );
    }

    /// Spawn all tasks responsible to handle messages from our primary.
    fn handle_primary_messages(&self) {
        let (tx_synchronizer, rx_synchronizer) = channel(CHANNEL_CAPACITY);

        // Receive incoming messages from our primary.
        let mut address = self
            .committee
            .worker(&PK(self.name.to_bytes()), &self.id)
            .expect("Our public key or worker id is not in the committee")
            .primary_to_worker;
        address.set_ip("0.0.0.0".parse().unwrap());
        Receiver::spawn(
            address,
            /* handler */
            PrimaryReceiverHandler { tx_synchronizer },
        );

        // The `Synchronizer` is responsible to keep the worker in sync with the others. It handles the commands
        // it receives from the primary (which are mainly notifications that we are out of sync).
        Synchronizer::spawn(
            self.name.clone(),
            self.id,
            self.committee.clone(),
            self.store.clone(),
            self.parameters.gc_depth,
            self.parameters.sync_retry_delay,
            self.parameters.sync_retry_nodes,
            /* rx_message */ rx_synchronizer,
        );

        info!(
            "Worker {} listening to primary messages on {}",
            self.id, address
        );
    }

    /// Spawn all tasks responsible to handle clients transactions.
    fn handle_clients_transactions(&self, tx_primary: Sender<SerializedBatchDigestMessage>, primary_address: SocketAddr) {
        let (tx_batch_maker, rx_batch_maker) = channel(CHANNEL_CAPACITY);
        let (tx_quorum_waiter, rx_quorum_waiter) = channel(CHANNEL_CAPACITY);
        let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY);

        let mut R: Vec<RistrettoPoint> = vec![RistrettoPoint::identity(); RING_SIZE];
                let mut x: Scalar = Scalar::one();

                for i in 0..RING_SIZE {
                    let (sk, pk) = KeyGen();
                    R[i] = pk;

                    if i == RING_SIZE {
                        x = sk;
                    }
                }

        // We first receive clients' transactions from the network.
        let mut address = self
            .committee
            .worker(&PK(self.name.to_bytes()), &self.id)
            .expect("Our public key or worker id is not in the committee")
            .transactions;
        address.set_ip("0.0.0.0".parse().unwrap());
        Receiver::spawn(
            address,
            /* handler */ TxReceiverHandler { tx_batch_maker, R },
        );

        // The transactions are sent to the `BatchMaker` that assembles them into batches. It then broadcasts
        // (in a reliable manner) the batches to all other workers that share the same `id` as us. Finally, it
        // gathers the 'cancel handlers' of the messages and send them to the `QuorumWaiter`.
        BatchMaker::spawn(
            self.parameters.batch_size,
            self.parameters.max_batch_delay,
            /* rx_transaction */ rx_batch_maker,
            /* tx_message */ tx_quorum_waiter,
            /* workers_addresses */
            self.committee
                .others_workers(&PK(self.name.to_bytes()), &self.id)
                .iter()
                .map(|(name, addresses)| (PublicKey::from_bytes(name.0), addresses.worker_to_worker))
                .collect(),
            primary_address,
            tx_processor
        );

        // The `QuorumWaiter` waits for 2f authorities to acknowledge reception of the batch. It then forwards
        // the batch to the `Processor`.
        /*QuorumWaiter::spawn(
            self.committee.clone(),
            /* stake */ self.committee.stake(&PK(self.name.to_bytes())),
            /* rx_message */ rx_quorum_waiter,
            /* tx_batch */ tx_processor,
        );*/

        // The `Processor` hashes and stores the batch. It then forwards the batch's digest to the `PrimaryConnector`
        // that will send it to our primary machine.
        Processor::spawn(
            self.id,
            self.store.clone(),
            /* rx_batch */ rx_processor,
            /* tx_digest */ tx_primary,
            /* own_batch */ true,
        );

        info!(
            "Worker {} listening to client transactions on {}",
            self.id, address
        );
    }

    /// Spawn all tasks responsible to handle messages from other workers.
    fn handle_workers_messages(&self, tx_primary: Sender<SerializedBatchDigestMessage>) {
        let (tx_helper, rx_helper) = channel(CHANNEL_CAPACITY);
        let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY);

        // Receive incoming messages from other workers.
        let mut address = self
            .committee
            .worker(&PK(self.name.to_bytes()), &self.id)
            .expect("Our public key or worker id is not in the committee")
            .worker_to_worker;
        address.set_ip("0.0.0.0".parse().unwrap());
        Receiver::spawn(
            address,
            /* handler */
            WorkerReceiverHandler {
                tx_helper,
                tx_processor,
            },
        );

        // The `Helper` is dedicated to reply to batch requests from other workers.
        Helper::spawn(
            self.id,
            self.committee.clone(),
            self.store.clone(),
            /* rx_request */ rx_helper,
        );

        // This `Processor` hashes and stores the batches we receive from the other workers. It then forwards the
        // batch's digest to the `PrimaryConnector` that will send it to our primary.
        Processor::spawn(
            self.id,
            self.store.clone(),
            /* rx_batch */ rx_processor,
            /* tx_digest */ tx_primary,
            /* own_batch */ false,
        );

        info!(
            "Worker {} listening to worker messages on {}",
            self.id, address
        );
    }
}

/// Defines how the network receiver handles incoming transactions.
#[derive(Clone)]
struct TxReceiverHandler {
    tx_batch_maker: Sender<Transaction>,
    R: Vec<RistrettoPoint>,
}

#[derive(Default, Clone, Deserialize, Serialize, Debug)]
pub struct Block {
    pub txs: Vec<Transaction>,
    //pub range_proof_bytes: Vec<u8>,
    //pub commitments: Vec<CompressedRistretto>,
}

#[async_trait]
impl MessageHandler for TxReceiverHandler {
    async fn dispatch(&self, _writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>> {
        //info!("TX received: {:?}", message);
        //let txs: Vec<Transaction> = bincode::deserialize(&message).unwrap();
        let tx: Transaction = bincode::deserialize(&message).unwrap();

        //let start2 = Instant::now();

                //for tx in block.txs {
                    Verify(&tx.signature, "msg", &self.R).unwrap();

		//let end2 = Instant::now();

		//let duration2 = Duration::as_millis(&(end2-start2));

		//info!("verification: {:?} ms", duration2);
                    check_range_proof(&RangeProof::from_bytes(&tx.range_proof_bytes).unwrap(), &tx.commitment, &PedersenGens::default(), &mut OsRng).unwrap();
                //}

        //for tx in txs {
            self.tx_batch_maker
                .send(tx)
                .await
                .expect("Failed to send transaction");
        //}

        // Give the change to schedule other tasks.
        tokio::task::yield_now().await;
        Ok(())
    }
}

/// Defines how the network receiver handles incoming workers messages.
#[derive(Clone)]
struct WorkerReceiverHandler {
    tx_helper: Sender<(Vec<Digest>, PublicKey)>,
    tx_processor: Sender<SerializedBatchMessage>,
}

#[async_trait]
impl MessageHandler for WorkerReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        // Reply with an ACK.
        let _ = writer.send(Bytes::from("Ack")).await;

        // Deserialize and parse the message.
        match bincode::deserialize(&serialized) {
            Ok(WorkerMessage::Batch(block)) => { 
                info!("Received block: {:?}", block);

                let mut R: Vec<RistrettoPoint> = vec![RistrettoPoint::identity(); RING_SIZE];
                let mut x: Scalar = Scalar::one();

                for i in 0..RING_SIZE {
                    let (sk, pk) = KeyGen();
                    R[i] = pk;

                    if i == 0 {
                        x = sk;
                    }
                }
                for tx in block.txs {
                    //Verify(&tx.signature, "msg", &R).unwrap();
                    //check_range_proof(&RangeProof::from_bytes(&tx.range_proof_bytes).unwrap(), &tx.commitment, &PedersenGens::default(), &mut OsRng).unwrap();
                }

                self
                    .tx_processor
                    .send(serialized.to_vec())
                    .await
                    .expect("Failed to send batch")
            }
            Ok(WorkerMessage::BatchRequest(missing, requestor)) => self
                .tx_helper
                .send((missing, requestor))
                .await
                .expect("Failed to send batch request"),
            Err(e) => warn!("Serialization error: {}", e),
        }
        Ok(())
    }
}

/// Defines how the network receiver handles incoming primary messages.
#[derive(Clone)]
struct PrimaryReceiverHandler {
    tx_synchronizer: Sender<PrimaryWorkerMessage>,
}

#[async_trait]
impl MessageHandler for PrimaryReceiverHandler {
    async fn dispatch(
        &self,
        _writer: &mut Writer,
        serialized: Bytes,
    ) -> Result<(), Box<dyn Error>> {
        // Deserialize the message and send it to the synchronizer.
        match bincode::deserialize(&serialized) {
            Err(e) => error!("Failed to deserialize primary message: {}", e),
            Ok(message) => self
                .tx_synchronizer
                .send(message)
                .await
                .expect("Failed to send transaction"),
        }
        Ok(())
    }
}

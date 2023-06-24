// Copyright(C) Facebook, Inc. and its affiliates.
use crate::core::{Core, TxHash};
use crate::election::ElectionId;
use crate::error::DagError;
use crate::messages::{Hash, Header};
use crate::payload_receiver::PayloadReceiver;
use crate::proposer::Proposer;
use async_trait::async_trait;
use bytes::Bytes;
use config::{Committee, Parameters};
use crypto::{Digest, PublicKey, SecretKey, SignatureService};
use ed25519_dalek::{Digest as _, Sha512};
use futures::sink::SinkExt as _;
use log::info;
use network::{MessageHandler, Receiver as NetworkReceiver, Writer};
use serde::{Deserialize, Serialize};
use std::convert::TryInto;
use std::error::Error;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::sync::mpsc::{channel, Sender};

/// The default channel capacity for each channel of the primary.
pub const CHANNEL_CAPACITY: usize = 100_000;

/// The round number.
pub type Round = u64;

#[derive(Debug, Serialize, Deserialize)]
pub enum PrimaryMessage {
    Header(Header),
}

/// The messages sent by the primary to its workers.
#[derive(Debug, Serialize, Deserialize)]
pub enum PrimaryWorkerMessage {
    /// The primary indicates that the worker need to sync the target missing batches.
    Synchronize(Vec<Digest>, /* target */ PublicKey),
    /// The primary indicates a round update.
    Cleanup(Round),
}

/// The messages sent by the workers to their primary.
#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerPrimaryMessage {
    /// The worker indicates it sealed a new batch.
    OurBatch(TxHash, ElectionId),
    /// The worker indicates it received a batch's digest from another authority.
    OthersBatch(TxHash, ElectionId),
}

pub struct Primary;

impl Primary {
    pub fn spawn(
        name: PublicKey,
        secret: SecretKey,
        committee: Committee,
        parameters: Parameters,
        //tx_consensus: Sender<Certificate>,
        //rx_consensus: Receiver<Certificate>,
    ) {
        let (tx_others_digests, rx_others_digests) = channel(CHANNEL_CAPACITY);
        let (tx_our_digests, rx_our_digests) = channel(CHANNEL_CAPACITY);
        let (tx_parents, rx_parents) = channel(CHANNEL_CAPACITY);
        let (tx_headers, rx_headers) = channel(CHANNEL_CAPACITY);
        let (tx_primary_messages, rx_primary_messages) = channel(CHANNEL_CAPACITY);

        // Write the parameters to the logs.
        parameters.log();

        // Atomic variable use to synchronizer all tasks with the latest consensus round. This is only
        // used for cleanup. The only tasks that write into this variable is `GarbageCollector`.
        let consensus_round = Arc::new(AtomicU64::new(0));

        // Spawn the network receiver listening to messages from the other primaries.
        let mut address = committee
            .primary(&name)
            .expect("Our public key or worker id is not in the committee")
            .primary_to_primary;
        address.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(
            address,
            /* handler */
            PrimaryReceiverHandler {
                tx_primary_messages,
            },
        );
        info!(
            "Primary {} listening to primary messages on {}",
            name, address
        );

        // Spawn the network receiver listening to messages from our workers.
        let mut address = committee
            .primary(&name)
            .expect("Our public key or worker id is not in the committee")
            .worker_to_primary;
        address.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(
            address,
            /* handler */
            WorkerReceiverHandler {
                tx_our_digests,
                tx_others_digests,
            },
        );
        info!(
            "Primary {} listening to workers messages on {}",
            name, address
        );

        // The `SignatureService` is used to require signatures on specific digests.
        let signature_service = SignatureService::new(secret);

        let addresses = committee
            .others_primaries(&name)
            .iter()
            .map(|(_, x)| x.primary_to_primary)
            .collect();

        // The `Core` receives and handles headers, votes, and certificates from the other primaries.
        Core::spawn(
            name.clone(),
            committee.clone(),
            signature_service.clone(),
            consensus_round.clone(),
            parameters.gc_depth,
            /* rx_primaries */ rx_primary_messages,
            /* rx_proposer */ rx_headers,
            /* tx_proposer */ tx_parents,
            addresses,
            committee.authorities.get(&name).unwrap().byzantine,
            parameters.header_size,
        );

        // Receives batch digests from other workers. They are only used to validate headers.
        PayloadReceiver::spawn(/* rx_workers */ rx_others_digests);

        // When the `Core` collects enough parent certificates, the `Proposer` generates a new header with new batch
        // digests from our workers and it back to the `Core`.
        Proposer::spawn(
            name.clone(),
            &committee,
            signature_service,
            parameters.header_size,
            parameters.max_header_delay,
            /* rx_core */ rx_parents,
            /* rx_workers */ rx_our_digests,
            /* tx_core */ tx_headers,
        );

        // NOTE: This log entry is used to compute performance.
        info!(
            "Primary {} successfully booted on {}",
            name.clone(),
            committee
                .primary(&name)
                .expect("Our public key or worker id is not in the committee")
                .primary_to_primary
                .ip()
        );
    }
}

/// Defines how the network receiver handles incoming primary messages.
#[derive(Clone)]
struct PrimaryReceiverHandler {
    tx_primary_messages: Sender<PrimaryMessage>,
}

#[async_trait]
impl MessageHandler for PrimaryReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        // Reply with an ACK.
        let _ = writer.send(Bytes::from("Ack")).await;

        // Deserialize and parse the message.
        match bincode::deserialize(&serialized).map_err(DagError::SerializationError)? {
            request => self
                .tx_primary_messages
                .send(request)
                .await
                .expect("Failed to send certificate"),
        }
        Ok(())
    }
}

/// Defines how the network receiver handles incoming workers messages.
#[derive(Clone)]
struct WorkerReceiverHandler {
    tx_our_digests: Sender<(TxHash, ElectionId)>,
    tx_others_digests: Sender<(TxHash, ElectionId)>,
}

#[async_trait]
impl MessageHandler for WorkerReceiverHandler {
    async fn dispatch(
        &self,
        _writer: &mut Writer,
        serialized: Bytes,
    ) -> Result<(), Box<dyn Error>> {
        // Deserialize and parse the message.
        match bincode::deserialize(&serialized).map_err(DagError::SerializationError)? {
            WorkerPrimaryMessage::OurBatch(digest, election_id) => self
                .tx_our_digests
                .send((digest, election_id))
                .await
                .expect("Failed to send workers' digests"),
            WorkerPrimaryMessage::OthersBatch(digest, election_id) => self
                .tx_others_digests
                .send((digest, election_id))
                .await
                .expect("Failed to send workers' digests"),
        }
        Ok(())
    }
}

//pub type Transaction = Vec<u8>;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Transaction {
    pub data: Vec<u8>,
    pub id: Vec<u8>,
}

impl Transaction {
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            id: Vec::new(),
        }
    }
}

impl Hash for Transaction {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(&self.data);
        hasher.update(&self.id);
        hasher.finalize().as_slice()[..32].try_into().unwrap()
    }
}

pub type Batch = Vec<Transaction>;

// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::SerializedBatchDigestMessage;
use config::WorkerId;
use crypto::Digest;
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;

use primary::WorkerPrimaryMessage;
use std::convert::TryInto;
use tokio::sync::mpsc::{Receiver, Sender};

/// Indicates a serialized `WorkerMessage::Batch` message.
pub type SerializedBatchMessage = Vec<u8>;

/// Hashes and stores batches, it then outputs the batch's digest.
pub struct Processor;

impl Processor {
    pub fn spawn(
        // Our worker's id.
        _id: WorkerId,
        // Input channel to receive batches.
        mut rx_batch: Receiver<(SerializedBatchMessage, Digest)>,
        // Output channel to send out batches' digests.
        tx_digest: Sender<SerializedBatchDigestMessage>,
        // Whether we are processing our own batches or the batches of other nodes.
        own_digest: bool,
    ) {
        tokio::spawn(async move {
            while let Some((batch, election_id)) = rx_batch.recv().await {
                //info!("id: {:?}", election_id);
                // validate txs
                //let txs: Vec<Transaction> = bincode::deserialize(&batch).unwrap();

                // Hash the batch.
                let digest = Digest(Sha512::digest(&batch).as_slice()[..32].try_into().unwrap());

                // Store the batch.
                //store.write(digest.to_vec(), batch).await;

                // Deliver the batch's digest.
                let message = match own_digest {
                    true => WorkerPrimaryMessage::OurBatch(digest, election_id),
                    false => WorkerPrimaryMessage::OthersBatch(digest, election_id),
                };
                let message = bincode::serialize(&message)
                    .expect("Failed to serialize our own worker-primary message");
                tx_digest
                    .send(message)
                    .await
                    .expect("Failed to send digest");
            }
        });
    }
}

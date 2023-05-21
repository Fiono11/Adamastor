use crate::Block;
// Copyright(C) Facebook, Inc. and its affiliates.
use crate::quorum_waiter::QuorumWaiterMessage;
use crate::worker::WorkerMessage;
use bulletproofs_og::PedersenGens;
use bytes::Bytes;
use chacha20poly1305::aead::{Aead, OsRng};
use chacha20poly1305::{Key, ChaCha20Poly1305, Nonce, KeyInit};
use curve25519_dalek::constants::RISTRETTO_BASEPOINT_POINT;
//#[cfg(feature = "benchmark")]
//use crypto::Digest;
use mc_account_keys::{PublicAddress as PublicKey, AccountKey};
use mc_crypto_keys::RistrettoPublic;
use mc_crypto_keys::tx_hash::TxHash as Digest;
#[cfg(feature = "benchmark")]
use ed25519_dalek::{Digest as _, Sha512};
#[cfg(feature = "benchmark")]
use log::info;
use mc_crypto_ring_signature::Scalar;
use mc_crypto_ring_signature::onetime_keys::create_shared_secret;
use mc_transaction_core::range_proofs::generate_range_proofs;
use mc_transaction_core::tx::Transaction;
use network::ReliableSender;
#[cfg(feature = "benchmark")]
use std::convert::TryInto as _;
use std::net::SocketAddr;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/batch_maker_tests.rs"]
pub mod batch_maker_tests;

//pub type Transaction = Vec<u8>;
pub type Batch = Vec<Transaction>;

/// Assemble clients transactions into batches.
pub struct BatchMaker {
    /// The preferred batch size (in bytes).
    batch_size: usize,
    /// The maximum delay after which to seal the batch (in ms).
    max_batch_delay: u64,
    /// Channel to receive transactions from the network.
    rx_transaction: Receiver<Transaction>,
    /// Output channel to deliver sealed batches to the `QuorumWaiter`.
    tx_message: Sender<QuorumWaiterMessage>,
    /// The network addresses of the other workers that share our worker id.
    workers_addresses: Vec<(PublicKey, SocketAddr)>,
    /// Holds the current batch.
    current_batch: Batch,
    /// Holds the size of the current batch (in bytes).
    current_batch_size: usize,
    /// A network sender to broadcast the batches to the other workers.
    network: ReliableSender,
}

impl BatchMaker {
    pub fn spawn(
        batch_size: usize,
        max_batch_delay: u64,
        rx_transaction: Receiver<Transaction>,
        tx_message: Sender<QuorumWaiterMessage>,
        workers_addresses: Vec<(PublicKey, SocketAddr)>,
    ) {
        tokio::spawn(async move {
            Self {
                batch_size,
                max_batch_delay,
                rx_transaction,
                tx_message,
                workers_addresses,
                current_batch: Batch::with_capacity(batch_size * 2),
                current_batch_size: 0,
                network: ReliableSender::new(),
            }
            .run()
            .await;
        });
    }

    /// Main loop receiving incoming transactions and creating batches.
    async fn run(&mut self) {
        let timer = sleep(Duration::from_millis(self.max_batch_delay));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                // Assemble client transactions into batches of preset size.
                Some(transaction) = self.rx_transaction.recv() => {
                    self.current_batch_size += transaction.len();
                    //info!("tx size: {:?}", transaction.len());
                    self.current_batch.push(transaction);
                    if self.current_batch_size >= self.batch_size {
                        self.seal().await;
                        timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                    }
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer => {
                    if !self.current_batch.is_empty() {
                        self.seal().await;
                    }
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                }
            }

            // Give the change to schedule other tasks.
            tokio::task::yield_now().await;
        }
    }

    /// Seal and broadcast the current batch.
    async fn seal(&mut self) {
        //info!("Current batch: {:?}", self.current_batch);

        #[cfg(feature = "benchmark")]
        let size = self.current_batch_size;

        // Look for sample txs (they all start with 0) and gather their txs id (the next 8 bytes).
        #[cfg(feature = "benchmark")]
        let tx_ids: Vec<_> = self
            .current_batch
            .iter()
            .filter(|tx| tx.id[0] == 0u8 && tx.id.len() > 8)
            .filter_map(|tx| tx.id[1..9].try_into().ok())
            .collect();

        //info!("tx_ids: {:?}", tx_ids);

        // Serialize the batch.
        self.current_batch_size = 0;
        let batch: Vec<Transaction> = self.current_batch.drain(..).collect();

        // create range proofs
        /*let mut rng = rand::rngs::OsRng;
        let generators = PedersenGens::default();
        let rep_account = AccountKey::default();
        let mut amounts = Vec::new();
        let mut blindings = Vec::new();

        for tx in &batch {
            let ss = create_shared_secret(&RistrettoPublic::from(tx.prefix.outputs[0].aux.0.decompress().unwrap()), rep_account.spend_private_key());
            let aC_bytes = ss.0.compress();
            let key2 = Key::from_slice(aC_bytes.as_bytes());
            let cipher2 = ChaCha20Poly1305::new(&key2);
            let nonce = Nonce::default();
            let plaintext2 = cipher2.decrypt(&nonce, tx.prefix.outputs[0].cipher_representative.as_ref()).unwrap();

            let mut bytes = [0; 32];
            bytes.copy_from_slice(&plaintext2[..]);

            let ss = Scalar::from_bits(bytes) * RISTRETTO_BASEPOINT_POINT;

            let (amount, blinding) = tx.prefix.outputs[0].masked_amount.get_value(&RistrettoPublic::from(ss)).unwrap();
            assert_eq!(amount.value, 1);
            amounts.push(amount.value);
            blindings.push(blinding);
        }*/

        //let (range_proof, commitments) = generate_range_proofs(&amounts, &blindings, &generators, &mut OsRng).unwrap();
        let block = Block {
            txs: batch,
            //range_proof_bytes: range_proof.to_bytes(),
            //commitments,
        };
        let message = WorkerMessage::Batch(block);
        let serialized = bincode::serialize(&message).expect("Failed to serialize our own batch");

        #[cfg(feature = "benchmark")]
        {
            // NOTE: This is one extra hash that is only needed to print the following log entries.
            let digest = Digest(
                Sha512::digest(&serialized).as_slice()[..32]
                    .try_into()
                    .unwrap(),
            );

            for id in tx_ids {
                // NOTE: This log entry is used to compute performance.
                info!(
                    "Batch {:?} contains sample tx {}",
                    digest,
                    u64::from_be_bytes(id)
                );
            }

            // NOTE: This log entry is used to compute performance.
            info!("Batch {:?} contains {} B", digest, size,);
        }

        // Broadcast the batch through the network.
        let (names, addresses): (Vec<_>, _) = self.workers_addresses.iter().cloned().unzip();
        let bytes = Bytes::from(serialized.clone());
        let handlers = self.network.broadcast(addresses, bytes).await;

        // Send the batch through the deliver channel for further processing.
        self.tx_message
            .send(QuorumWaiterMessage {
                batch: serialized,
                handlers: names.into_iter().zip(handlers.into_iter()).collect(),
            })
            .await
            .expect("Failed to deliver batch");
    }
}

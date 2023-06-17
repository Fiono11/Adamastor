use crate::core::TxHash;
use crate::election::ElectionId;
use crate::messages::{Header, Vote};
use crate::primary::Round;
use config::Committee;
use crypto::{Digest, PublicKey, SignatureService};

//#[cfg(feature = "benchmark")]
//use log::info;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

/// The proposer creates new headers and send them to the core for broadcasting and further processing.
pub struct Proposer {
    /// The public key of this primary.
    name: PublicKey,
    /// Service to sign headers.
    signature_service: SignatureService,
    /// The size of the headers' payload.
    header_size: usize,
    /// The maximum delay to wait for batches' digests.
    max_header_delay: u64,

    /// Receives the parents to include in the next header (along with their round number).
    rx_core: Receiver<(Vec<Digest>, Round)>,
    /// Receives the batches' digests from our workers.
    rx_workers: Receiver<(TxHash, ElectionId)>,
    /// Sends newly created headers to the `Core`.
    tx_core: Sender<Header>,

    /// The current round of the dag.
    round: Round,
    /// Holds the batches' digests waiting to be included in the next header.
    digests: Vec<(TxHash, ElectionId)>,
    /// Keeps track of the size (in bytes) of batches' digests that we received so far.
    payload_size: usize,
    votes: Vec<Vote>,
    active_elections: Vec<Digest>,
}

impl Proposer {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        _committee: &Committee,
        signature_service: SignatureService,
        header_size: usize,
        max_header_delay: u64,
        rx_core: Receiver<(Vec<Digest>, Round)>,
        rx_workers: Receiver<(TxHash, ElectionId)>,
        tx_core: Sender<Header>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                signature_service,
                header_size,
                max_header_delay,
                rx_core,
                rx_workers,
                tx_core,
                round: 1,
                digests: Vec::with_capacity(2 * header_size),
                payload_size: 0,
                votes: Vec::with_capacity(header_size),
                active_elections: Vec::new(),
            }
            .run()
            .await;
        });
    }

    async fn make_header(&mut self) {
        // Make a new header.
        let header = Header::new(
            self.name.clone(),
            self.votes.drain(..).collect(),
            &mut self.signature_service,
        )
        .await;

        //info!("Votes: {:?}", header.votes);
        //debug!("Created {:?}", header);

        //#[cfg(feature = "benchmark")]
        //for vote in &header.votes {
        // NOTE: This log entry is used to compute performance.
        //info!("Created {} -> {:?}", &vote, vote.election_id);
        //}

        // Send the new header to the `Core` that will broadcast and process it.
        self.tx_core
            .send(header)
            .await
            .expect("Failed to send header");
    }

    // Main loop listening to incoming messages.
    pub async fn run(&mut self) {
        //debug!("Dag starting at round {}", self.round);

        let timer = sleep(Duration::from_millis(self.max_header_delay));
        tokio::pin!(timer);

        loop {
            // Check if we can propose a new header. We propose a new header when one of the following
            // conditions is met:
            // 1. We have a quorum of certificates from the previous round and enough batches' digests;
            // 2. We have a quorum of certificates from the previous round and the specified maximum
            // inter-header delay has passed.
            //let enough_parents = !self.last_parents.is_empty();
            //let enough_digests = self.payload_size >= self.header_size;
            //let enough_digests = self.digests.len() == 1;
            let timer_expired = timer.is_elapsed();
            let enough_votes = self.votes.len() >= self.header_size;
            //info!("Digests: {:?}", self.digests);

            if enough_votes || timer_expired {
                // Make a new header.
                self.make_header().await;
                //self.payload_size = 0;

                // Reschedule the timer.
                let deadline = Instant::now() + Duration::from_millis(self.max_header_delay);
                timer.as_mut().reset(deadline);
            }

            tokio::select! {
                Some((tx_hash, election_id)) = self.rx_workers.recv() => {
                    if !self.active_elections.contains(&election_id) {
                        self.active_elections.push(election_id.clone());
                        let vote = Vote::new(0, tx_hash, election_id, false).await;
                        self.votes.push(vote);
                    }
                }
                () = &mut timer => {
                    // Nothing to do.
                }
            }
        }
    }
}

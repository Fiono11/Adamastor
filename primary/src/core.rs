use crate::constants::{QUORUM, SEMI_QUORUM, NUMBER_OF_NODES};
use crate::election::{self, Election, Tally, ElectionId, Timer};
// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::{DagError, DagResult};
use crate::messages::{Header, Vote};
use crate::primary::{PrimaryMessage, Round};
use async_recursion::async_recursion;
use bytes::Bytes;
use config::Committee;
use crypto::Hash as _;
use crypto::{Digest, PublicKey as PublicAddress, SignatureService};
use log::{debug, error, warn, info};
use network::{CancelHandler, ReliableSender, SimpleSender};
use rand::rngs::OsRng;
use rand::seq::IteratorRandom;
use std::collections::{HashMap, HashSet, BTreeSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/core_tests.rs"]
pub mod core_tests;

pub type TxHash = Digest;

pub struct Core {
    /// The public key of this primary.
    name: PublicAddress,
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    store: Store,
    /// Service to sign headers.
    signature_service: SignatureService,
    /// The current consensus round (used for cleanup).
    consensus_round: Arc<AtomicU64>,
    /// The depth of the garbage collector.
    gc_depth: Round,

    /// Receiver for dag messages (headers, votes, certificates).
    rx_primaries: Receiver<PrimaryMessage>,
    /// Receives our newly created headers from the `Proposer`.
    rx_proposer: Receiver<Header>,
    /// Send valid a quorum of certificates' ids to the `Proposer` (along with their round).
    tx_proposer: Sender<(Vec<TxHash>, Round)>,

    /// The last garbage collected round.
    gc_round: Round,
    /// The authors of the last voted headers.
    last_voted: HashMap<Round, HashSet<PublicAddress>>,
    /// The set of headers we are currently processing.
    processing: HashMap<Round, HashSet<TxHash>>,
    /// The last header we proposed (for which we are waiting votes).
    current_header: Header,
    /// A network sender to send the batches to the other workers.
    network: SimpleSender,
    /// Keeps the cancel handlers of the messages we sent.
    cancel_handlers: HashMap<Round, Vec<CancelHandler>>,
    elections: HashMap<ElectionId, Election>,
    addresses: Vec<SocketAddr>,
    byzantine: bool,
    payloads: HashMap<ElectionId, BTreeSet<TxHash>>,
    votes: Vec<Vote>,
    header_size: usize,
}

impl Core {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicAddress,
        committee: Committee,
        store: Store,
        signature_service: SignatureService,
        consensus_round: Arc<AtomicU64>,
        gc_depth: Round,
        rx_primaries: Receiver<PrimaryMessage>,
        rx_proposer: Receiver<Header>,
        tx_proposer: Sender<(Vec<TxHash>, Round)>,
        addresses: Vec<SocketAddr>,
        byzantine: bool,
        header_size: usize,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                store,
                signature_service,
                consensus_round,
                gc_depth,
                rx_primaries,
                rx_proposer,
                tx_proposer,
                gc_round: 0,
                last_voted: HashMap::with_capacity(2 * gc_depth as usize),
                processing: HashMap::with_capacity(2 * gc_depth as usize),
                current_header: Header::default(),
                network: SimpleSender::new(),
                cancel_handlers: HashMap::with_capacity(2 * gc_depth as usize),
                elections: HashMap::new(),
                addresses,
                byzantine,
                payloads: HashMap::new(),
                votes: Vec::new(),
                header_size,
            }
            .run()
            .await;
        });
    }

    #[async_recursion]
    async fn process_header(&mut self, header: &Header) -> DagResult<()> {
        for vote in &header.votes {
            if !vote.commit {
                //info!("Received vote {:?} from {}", vote, header.author);
            }
            else {
                //info!("Received commit {:?} from {}", vote, header.author);
            }
            let (tx_hash, election_id) = (vote.tx_hash.clone(), vote.election_id.clone()); 
            if !self.byzantine {
            //for (tx_hash, election_id) in &header.payload {
                match self.elections.get_mut(&election_id) {
                    Some(election) => {
                        election.insert_vote(&vote, header.author);
                        /*if header.author == self.name {
                            // broadcast vote
                            let bytes = bincode::serialize(&PrimaryMessage::Header(header.clone()))
                                .expect("Failed to serialize our own header");
                            let handlers = self.network.broadcast(self.addresses.clone(), Bytes::from(bytes)).await;
                            self.cancel_handlers
                                .entry(header.round)
                                .or_insert_with(Vec::new)
                                .extend(handlers);
                            info!("Sending vote: {:?}", header);
                        }*/
                    }
                    None => {
                        // create election
                        let election = Election::new();
                        self.elections.insert(election_id.clone(), election);

                        #[cfg(feature = "benchmark")]
                        // NOTE: This log entry is used to compute performance.
                        info!("Created {} -> {:?}", vote, election_id);
                            
                        let mut election = self.elections.get_mut(&election_id).unwrap();
                        // insert vote
                        election.insert_vote(&vote, header.author);

                        if header.author == self.name {
                            // broadcast vote
                            let bytes = bincode::serialize(&PrimaryMessage::Header(header.clone()))
                                .expect("Failed to serialize our own header");
                            let handlers = self.network.broadcast(self.addresses.clone(), Bytes::from(bytes)).await;
                            /*self.cancel_handlers
                                .entry(header.round)
                                .or_insert_with(Vec::new)
                                .extend(handlers);*/
                            //info!("Sending vote after new election: {:?}", header);
                        }
                    }
                }
                //let mut own_header = Header::default();
                // decide vote
                let election = self.elections.get_mut(&election_id).unwrap();
                //match election.tallies.get(&header.round) {
                    if let Some(tally) = election.tallies.get(&vote.election_round) {
                        if let Some(tx_hash) = tally.find_quorum_of_commits() {
                            if !election.decided {
                                #[cfg(not(feature = "benchmark"))]
                                info!("Committed {}", vote);
                                                    
                                #[cfg(feature = "benchmark")]
                                // NOTE: This log entry is used to compute performance.
                                info!("Committed {} -> {:?}", vote, election_id);
                                election.decided = true;
                            }
                            return Ok(());
                        }
                        //if !election.committed {
                            //own_header = header.clone();
                            if let Some((tx_hash, round)) = tally.find_quorum_of_votes() {
                                if !election.voted_or_committed(&self.name, vote.election_round+1) {
                                    /*let mut consensus_round = round;
                                    if let Some(hr) = election.highest_round {
                                        consensus_round = hr;
                                    }*/
                                    election.commit = Some(tx_hash.clone());
                                    election.proof_round = Some(vote.election_round);
                                    let vote = Vote::new(vote.election_round + 1, tx_hash.clone(), election_id, true, *round).await;
                                    election.insert_vote(&vote, self.name);

                                    self.votes.push(vote);
                                    //own_header.payload = (tx_hash.clone(), election_id.clone());
                                    //own_header.round = header.round + 1;
                                    //own_header.author = self.name;
                                    //own_header.commit = true;
                                    //election.committed = true;


                                    // broadcast vote
                                    //let bytes = bincode::serialize(&PrimaryMessage::Header(own_header.clone()))
                                        //.expect("Failed to serialize our own header");
                                    //let handlers = self.network.broadcast(self.addresses.clone(), Bytes::from(bytes)).await;
                                    /*self.cancel_handlers
                                        .entry(own_header.round)
                                        .or_insert_with(Vec::new)
                                        .extend(handlers);*/
                                    //info!("Sending commit: {:?}", own_header);
                                }
                            }
                            else if election.voted_or_committed(&self.name, vote.election_round) && ((tally.total_votes() >= QUORUM && *tally.timer.0.lock().unwrap() == Timer::Expired) || tally.total_votes() == NUMBER_OF_NODES)
                            && !election.voted_or_committed(&self.name, vote.election_round + 1) {
                                let highest = election.highest.clone().unwrap();
                                let mut consensus_round = vote.consensus_round;
                                if let Some(hr) = election.highest_round {
                                    consensus_round = hr;
                                }
                                //own_header.payload = (highest.clone(), election_id.clone());
                                //own_header.round = header.round + 1;
                                //own_header.author = self.name;
                                let vote = Vote::new(vote.election_round+1, highest, election_id, false, consensus_round).await;
                                self.votes.push(vote.clone());
                                //election.round = header.round + 1;
                                election.insert_vote(&vote, self.name);

                                // broadcast vote
                                //let bytes = bincode::serialize(&PrimaryMessage::Header(own_header.clone()))
                                    //.expect("Failed to serialize our own header");
                                //let handlers = self.network.broadcast(self.addresses.clone(), Bytes::from(bytes)).await;
                                /*self.cancel_handlers
                                    .entry(own_header.round)
                                    .or_insert_with(Vec::new)
                                    .extend(handlers);*/
                                //info!("Changing vote: {:?}", own_header);
                            }
                            else if !election.voted_or_committed(&self.name, vote.election_round) {
                                    let mut tx_hash = tx_hash;
                                    if let Some(highest) = &election.highest {
                                        tx_hash = highest.clone();
                                    }
                                    //election.voted = true;
                                    //election.round = header.round + 1;
                                    //own_header.author = self.name;

                                    let mut consensus_round = vote.consensus_round;
                                    if let Some(hr) = election.highest_round {
                                        consensus_round = hr;
                                    }

                                    let vote = Vote::new(vote.election_round, tx_hash, election_id, vote.commit, consensus_round).await;
                                    election.insert_vote(&vote, self.name);
                                    self.votes.push(vote);

                                    // broadcast vote
                                    //let bytes = bincode::serialize(&PrimaryMessage::Header(own_header.clone()))
                                        //.expect("Failed to serialize our own header");
                                    //let handlers = self.network.broadcast(self.addresses.clone(), Bytes::from(bytes)).await;
                                    /*self.cancel_handlers
                                        .entry(own_header.round)
                                        .or_insert_with(Vec::new)
                                        .extend(handlers);*/
                                    //info!("Sending vote: {:?}", own_header);                            
                            }
                        //}               
                    //}
                }
                //info!("Election of {:?}: {:?}", &election_id, self.elections.get(&election_id).unwrap());
            }
            else {
                match self.payloads.get_mut(&election_id) {
                    Some(txs) => {
                        txs.insert(tx_hash.clone());
                    }
                    None => {
                        let mut txs = BTreeSet::new();
                        txs.insert(tx_hash.clone());
                        self.payloads.insert(election_id.clone(), txs);
                    }
                }
                let mut rng = OsRng;
                let digest = self.payloads.get(&election_id).unwrap().iter().choose(&mut rng).unwrap().clone();
                //let digest = Digest::random();
                let vote = Vote::new(vote.election_round, digest, election_id, rand::random(), vote.consensus_round).await;
                self.votes.push(vote);
                //let own_header = Header::new(self.name, header.round, payload, &mut self.signature_service, rand::random()).await;
                // broadcast vote
                //let bytes = bincode::serialize(&PrimaryMessage::Header(own_header.clone()))
                    //.expect("Failed to serialize our own header");
                //let handlers = self.network.broadcast(self.addresses.clone(), Bytes::from(bytes)).await;
                /*self.cancel_handlers
                    .entry(own_header.round)
                    .or_insert_with(Vec::new)
                    .extend(handlers);*/
                //info!("Sending vote: {:?}", own_header);
            }

            if self.votes.len() >= self.header_size {
                // broadcast votes
                let own_header = Header::new(self.name, self.votes.drain(..).collect(), &mut self.signature_service).await;
                let bytes = bincode::serialize(&PrimaryMessage::Header(own_header.clone()))
                    .expect("Failed to serialize our own header");
                let handlers = self.network.broadcast(self.addresses.clone(), Bytes::from(bytes)).await;
            }
        }
        Ok(())
    }

    /*#[async_recursion]
    async fn process_header(&mut self, header: &Header) -> DagResult<()> {
        //info!("name: {:?}", self.name);
        //info!("Received {:?} from {:?}", header, header.author);

        for (tx_hash, election_id) in &header.payload {
            if !header.commit {
                info!("Received vote of {:?} in {:?}", tx_hash, election_id);
                match self.elections.get_mut(&election_id) {
                    Some(election) => {
                        if let Some(tally) = election.tallies.get_mut(&header.round) {
                            if let Some(votes) = tally.votes.get_mut(&tx_hash) {
                                votes.insert(header.author.clone());
                                //self.payloads.insert(header.id.clone(), header.payload.clone());
                                if header.author == self.name {
                                    // Broadcast the new header in a reliable manner.
                                    let addresses = self
                                        .committee
                                        .others_primaries(&self.name)
                                        .iter()
                                        .map(|(_, x)| x.primary_to_primary)
                                        .collect();
                                    let bytes = bincode::serialize(&PrimaryMessage::Header(header.clone()))
                                        .expect("Failed to serialize our own header");
                                    let handlers = self.network.broadcast(addresses, Bytes::from(bytes)).await;
                                    self.cancel_handlers
                                        .entry(header.round)
                                        .or_insert_with(Vec::new)
                                        .extend(handlers);
                                    info!("Sending vote: {:?}", header);
                                }
                                if !votes.contains(&self.name) && !election.decided {
                                    let mut own_header = header.clone();
                                    own_header.author = self.name.clone();
                                    //let mut payload = BTreeSet::new();
                                    //payload.insert(header.id.clone());
                                    //own_header.payload = payload;
                                    // Broadcast the new header in a reliable manner.
                                    let addresses = self
                                        .committee
                                        .others_primaries(&self.name)
                                        .iter()
                                        .map(|(_, x)| x.primary_to_primary)
                                        .collect();
                                    let bytes = bincode::serialize(&PrimaryMessage::Header(own_header.clone()))
                                        .expect("Failed to serialize our own header");
                                    let handlers = self.network.broadcast(addresses, Bytes::from(bytes)).await;
                                    self.cancel_handlers
                                        .entry(header.round)
                                        .or_insert_with(Vec::new)
                                        .extend(handlers);
                                    votes.insert(own_header.author.clone());
                                    info!("Sending vote: {:?}", own_header);
                                }
                                if votes.len() >= QUORUM && !election.decided {
                                    let mut own_header = header.clone();
                                    own_header.author = self.name.clone();
                                    //let mut payload = BTreeSet::new();
                                    //payload.insert(header.id.clone());
                                    //own_header.payload = payload;
                                    own_header.commit = true;
                                    //tally.commits.insert((own_header.author.clone(), tx_hash.clone()));
                                    // Broadcast the new header in a reliable manner.
                                    let addresses = self
                                        .committee
                                        .others_primaries(&self.name)
                                        .iter()
                                        .map(|(_, x)| x.primary_to_primary)
                                        .collect();
                                    let bytes = bincode::serialize(&PrimaryMessage::Header(own_header.clone()))
                                        .expect("Failed to serialize our own header");
                                    let handlers = self.network.broadcast(addresses, Bytes::from(bytes)).await;
                                    self.cancel_handlers
                                        .entry(header.round)
                                        .or_insert_with(Vec::new)
                                        .extend(handlers);
                                    info!("Sending commit: {:?}", own_header);
                                }
                            }
                            else {
                                let mut btreeset = BTreeSet::new();
                                btreeset.insert(header.author.clone());
                                tally.votes.insert(tx_hash.clone(), btreeset);
                                //self.payloads.insert(header.id.clone(), header.payload.clone());
                                if header.author == self.name {
                                    // Broadcast the new header in a reliable manner.
                                    let addresses = self
                                        .committee
                                        .others_primaries(&self.name)
                                        .iter()
                                        .map(|(_, x)| x.primary_to_primary)
                                        .collect();
                                    let bytes = bincode::serialize(&PrimaryMessage::Header(header.clone()))
                                        .expect("Failed to serialize our own header");
                                    let handlers = self.network.broadcast(addresses, Bytes::from(bytes)).await;
                                    self.cancel_handlers
                                        .entry(header.round)
                                        .or_insert_with(Vec::new)
                                        .extend(handlers);
                                    info!("Sending vote: {:?}", header);
                                }
                                if let Some(votes) = tally.votes.get_mut(&tx_hash) {
                                    if !votes.contains(&self.name) && !election.decided {
                                        let mut own_header = header.clone();
                                        own_header.author = self.name.clone();
                                        //let mut payload = BTreeSet::new();
                                        //payload.insert(header.id.clone());
                                        //own_header.payload = payload;
                                        // Broadcast the new header in a reliable manner.
                                        let addresses = self
                                            .committee
                                            .others_primaries(&self.name)
                                            .iter()
                                            .map(|(_, x)| x.primary_to_primary)
                                            .collect();
                                        let bytes = bincode::serialize(&PrimaryMessage::Header(own_header.clone()))
                                            .expect("Failed to serialize our own header");
                                        let handlers = self.network.broadcast(addresses, Bytes::from(bytes)).await;
                                        self.cancel_handlers
                                            .entry(header.round)
                                            .or_insert_with(Vec::new)
                                            .extend(handlers);
                                        votes.insert(own_header.author.clone());
                                        info!("Sending vote: {:?}", own_header);
                                    }
                                }
                            }
                        }       
                    }
                    None => {
                        if header.author != self.name {
                            #[cfg(feature = "benchmark")]
                            for digest in &header.payload {
                                // NOTE: This log entry is used to compute performance.
                                info!("Created {} -> {:?}", header, digest.1);
                            }
                        }
                        let mut tally = Tally::new();
                        let mut votes = HashMap::new();
                        let mut btreeset = BTreeSet::new();
                        btreeset.insert(header.author.clone());
                        votes.insert(tx_hash.clone(), btreeset);
                        tally.votes = votes;
                        //tally.votes.insert((header.author.clone(), tx_hash.clone()));
                        //self.payloads.insert(header.id.clone(), header.payload.clone());
                        if header.author == self.name {
                            // Broadcast the new header in a reliable manner.
                            let addresses = self
                                .committee
                                .others_primaries(&self.name)
                                .iter()
                                .map(|(_, x)| x.primary_to_primary)
                                .collect();
                            let bytes = bincode::serialize(&PrimaryMessage::Header(header.clone()))
                                .expect("Failed to serialize our own header");
                            let handlers = self.network.broadcast(addresses, Bytes::from(bytes)).await;
                            self.cancel_handlers
                                .entry(header.round)
                                .or_insert_with(Vec::new)
                                .extend(handlers);
                            info!("Sending vote: {:?}", header);
                        }
                        if let Some(votes) = tally.votes.get_mut(&tx_hash) {
                            let mut own_header = header.clone();
                            own_header.author = self.name.clone();
                            // Broadcast the new header in a reliable manner.
                            let addresses = self
                                .committee
                                .others_primaries(&self.name)
                                .iter()
                                .map(|(_, x)| x.primary_to_primary)
                                .collect();
                            let bytes = bincode::serialize(&PrimaryMessage::Header(own_header.clone()))
                                .expect("Failed to serialize our own header");
                            let handlers = self.network.broadcast(addresses, Bytes::from(bytes)).await;
                            self.cancel_handlers
                                .entry(header.round)
                                .or_insert_with(Vec::new)
                                .extend(handlers);
                            votes.insert(own_header.author.clone());
                            info!("Sending vote: {:?}", own_header);
                        }
                        let mut election = Election::new();
                        election.tallies.insert(header.round, tally);
                        self.elections.insert(election_id.clone(), election);
                    }
                }
            }
            else {
                info!("Received commit of {:?} in {:?}", tx_hash, election_id);
                match self.elections.get_mut(&election_id) {
                    Some(election) => {
                        if let Some(tally) = election.tallies.get_mut(&header.round) {
                            if let Some(commits) = tally.commits.get_mut(&tx_hash) {
                                commits.insert(header.author.clone());
                                if !commits.contains(&self.name) && commits.len() >= SEMI_QUORUM && !election.decided {
                                    let mut own_header = header.clone();
                                    own_header.author = self.name.clone();
                                    own_header.commit = true;
                                    // Broadcast the new header in a reliable manner.
                                    let addresses = self
                                        .committee
                                        .others_primaries(&self.name)
                                        .iter()
                                        .map(|(_, x)| x.primary_to_primary)
                                        .collect();
                                    let bytes = bincode::serialize(&PrimaryMessage::Header(own_header.clone()))
                                        .expect("Failed to serialize our own header");
                                    let handlers = self.network.broadcast(addresses, Bytes::from(bytes)).await;
                                    self.cancel_handlers
                                        .entry(header.round)
                                        .or_insert_with(Vec::new)
                                        .extend(handlers);
                                    info!("Sending commit: {:?}", own_header);
                                    commits.insert(own_header.author.clone());
                                }
                                
                                if commits.len() >= QUORUM {
                                    #[cfg(not(feature = "benchmark"))]
                                    info!("Committed {}", header);

                                    for payload in &header.payload {
                                        //for digest in self.payloads.get(&payload).unwrap() {
                                            #[cfg(feature = "benchmark")]
                                            // NOTE: This log entry is used to compute performance.
                                            info!("Committed {} -> {:?}", header, payload.1);
                                        //}
                                    }
                                    election.decided = true;
                                }
                            }
                        }
                    }
                    None => {
                        let mut election = Election::new();
                        let mut tally = Tally::new();
                        let mut commits = HashMap::new();
                        let mut btreeset = BTreeSet::new();
                        btreeset.insert(header.author.clone());
                        commits.insert(tx_hash.clone(), btreeset);
                        tally.commits = commits;
                        election.tallies.insert(header.round, tally);
                        self.elections.insert(election_id.clone(), election);
                    }
                }
            }
            info!("Election of {:?}: {:?}", &election_id, self.elections.get(&election_id).unwrap());
        }
        Ok(())
    }*/

    // Main loop listening to incoming messages.
    pub async fn run(&mut self) {
        loop {
            let result = tokio::select! {
                // We receive here messages from other primaries.
                Some(message) = self.rx_primaries.recv() => {
                    match message {
                        PrimaryMessage::Header(header) => self.process_header(&header).await,
                        _ => panic!("Unexpected core message")
                    }
                },

                // We also receive here our new headers created by the `Proposer`.
                Some(header) = self.rx_proposer.recv() => self.process_header(&header).await,
            };
            match result {
                Ok(()) => (),
                Err(DagError::StoreError(e)) => {
                    error!("{}", e);
                    panic!("Storage failure: killing node.");
                }
                Err(e @ DagError::TooOld(..)) => debug!("{}", e),
                Err(e) => warn!("{}", e),
            }

            // Cleanup internal state.
            let round = self.consensus_round.load(Ordering::Relaxed);
            if round > self.gc_depth {
                let gc_round = round - self.gc_depth;
                self.last_voted.retain(|k, _| k >= &gc_round);
                self.processing.retain(|k, _| k >= &gc_round);
                self.cancel_handlers.retain(|k, _| k >= &gc_round);
                self.gc_round = gc_round;
            }
        }
    }
}

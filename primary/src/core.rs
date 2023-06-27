use crate::constants::{QUORUM, NUMBER_OF_NODES};
use crate::election::{Election, ElectionId, Timer};
// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::{DagError, DagResult};
use crate::messages::{Header, Vote};
use crate::primary::{PrimaryMessage, Round};
use async_recursion::async_recursion;
use bytes::Bytes;
use config::Committee;

use crypto::{Digest, PublicKey as PublicAddress, SignatureService};
use log::{debug, warn, info};
use network::{CancelHandler, SimpleSender};


use tokio::time::{sleep, Instant, Sleep};
use std::collections::{HashMap, HashSet, BTreeSet};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64};
use std::sync::{Arc};
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};

pub type TxHash = Digest;

const TIMER: u64 = 100;

pub struct Core {
    /// The public key of this primary.
    name: PublicAddress,
    /// The committee information.
    committee: Committee,
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
    decided: BTreeSet<ElectionId>,
    counter: u64,
}

impl Core {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicAddress,
        committee: Committee,
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
                decided: BTreeSet::new(),
                counter: 0,
            }
            .run()
            .await;
        });
    }

    #[async_recursion]
    async fn process_own_header(&mut self, header: &Header) -> DagResult<()> {
        assert!(header.author == self.name);
        //info!("Received own header with {} votes", header.votes.len());
            // broadcast vote
            let bytes = bincode::serialize(&PrimaryMessage::Header(header.clone()))
                .expect("Failed to serialize our own header");
            let _handlers = self.network.broadcast(self.addresses.clone(), Bytes::from(bytes)).await;

        for vote in &header.votes {
            if !vote.commit {
                //info!("Received vote {:?} from {}", vote, header.author);
            }
            else {
                //info!("Received commit {:?} from {}", vote, header.author);
            }
            let (_tx_hash, election_id) = (vote.tx_hash.clone(), vote.election_id.clone()); 
            if !self.byzantine {
                match self.elections.get_mut(&election_id) {
                    Some(election) => {
                        election.insert_vote(&vote, header.author);
                    }
                    None => {
                        // create election
                        let election = Election::new();
                        self.elections.insert(election_id.clone(), election);

                        //#[cfg(feature = "benchmark")]
                        // NOTE: This log entry is used to compute performance.
                        //info!("Created {} -> {:?}", vote, election_id);
                            
                        let election = self.elections.get_mut(&election_id).unwrap();
                        // insert vote
                        election.insert_vote(&vote, header.author);
                    }
                }
            }
        }
        Ok(())
    }

    #[async_recursion]
    async fn process_header(&mut self, header: &Header, timer: &mut Pin<&mut Sleep>) -> DagResult<()> {

        header.verify(&self.committee).unwrap();

        if header.author == self.name {
            //info!("Received own header with {} votes from {}", header.votes.len(), header.author);

            // broadcast header
            let bytes = bincode::serialize(&PrimaryMessage::Header(header.clone()))
                .expect("Failed to serialize our own header");
            let _handlers = self.network.broadcast(self.addresses.clone(), Bytes::from(bytes)).await;
        }
        else {
            //info!("Received header with {} votes from {}", header.votes.len(), header.author);
        }
  
        for vote in &header.votes {
            if !vote.commit {
                //info!("Received vote {:?} from {}", vote, header.author);
            }
            else {
                //info!("Received commit {:?} from {}", vote, header.author);
            }
            let (tx_hash, election_id) = (vote.tx_hash.clone(), vote.election_id.clone()); 
            if !self.byzantine {
                match self.elections.get_mut(&election_id) {
                    Some(election) => {
                        election.insert_vote(&vote, header.author);
                    }
                    None => {
                        // create election
                        let election = Election::new();
                        self.elections.insert(election_id.clone(), election);

                        //#[cfg(feature = "benchmark")]
                        // NOTE: This log entry is used to compute performance.
                        //info!("Created {} -> {:?}", vote, election_id);
                            
                        let election = self.elections.get_mut(&election_id).unwrap();
                        // insert vote
                        election.insert_vote(&vote, header.author);
                    }
                }

                // decide vote
                let election = self.elections.get_mut(&election_id).unwrap();
                if !election.decided {
                    if let Some(tally) = election.tallies.get(&vote.round) {

                        // reaches quorum of commits in this round
                        if let Some(_) = election.find_quorum_of_commits() {
                            //#[cfg(not(feature = "benchmark"))]
                            //info!("Committed {}", vote);

                            self.decided.insert(election_id.clone());

                            if self.decided.len() >= 1000 {
                                //#[cfg(feature = "benchmark")]
                                // NOTE: This log entry is used to compute performance.
                                info!("Committed {} -> {:?}", self.decided.len(), self.counter);

                                self.counter += 1;

                                self.decided = BTreeSet::new();

                            }     
                            
                            let deadline = Instant::now() + Duration::from_millis(TIMER);
                            timer.as_mut().reset(deadline);   
                          
                            election.decided = true;
                        }

                            // reaches quorum of votes in this round
                            if let Some(tx_hash) = tally.find_quorum_of_votes() {
                                if !election.voted_or_committed(&self.name, vote.round+1) {
                                    election.commit = Some(tx_hash.clone());
                                    election.proof_round = Some(vote.round);
                                    let vote = Vote::new(vote.round + 1, tx_hash.clone(), election_id, true).await;
                                    election.insert_vote(&vote, self.name);

                                    self.votes.push(vote);
                                }
                            }

                            // voted in this round already, not voted in the next round
                            else if election.voted_or_committed(&self.name, vote.round) && ((tally.total_votes() >= QUORUM && *tally.timer.0.lock().unwrap() == Timer::Expired) || tally.total_votes() == NUMBER_OF_NODES)
                            && !election.voted_or_committed(&self.name, vote.round + 1) {
                                let mut highest = election.highest.clone().unwrap();
                                let mut committed = false;

                                    if let Some(commit) = &election.commit {
                                        highest = commit.clone();
                                        committed = true;
                                    }
                                let vote = Vote::new(vote.round+1, highest, election_id, committed).await;
                                self.votes.push(vote.clone());
                                election.insert_vote(&vote, self.name);
                            }

                            // not voted in this round yet
                            else if !election.voted_or_committed(&self.name, vote.round) {
                                let mut tx_hash = tx_hash;
                                if let Some(highest) = &election.highest {
                                    tx_hash = highest.clone();
                                }
                                if let Some(commit) = &election.commit {
                                    tx_hash = commit.clone();
                                }
                                let vote = Vote::new(vote.round, tx_hash, election_id, vote.commit).await;
                                election.insert_vote(&vote, self.name);
                                self.votes.push(vote);                         
                            }
                        //}               
                    }
                }
                else {
                    self.elections.remove(&election_id);
                }
                //info!("Election of {:?}: {:?}", &election_id, self.elections.get(&election_id).unwrap());
            }
            else {
                /*match self.payloads.get_mut(&election_id) {
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
                let vote = Vote::new(vote.round, digest, election_id, rand::random()).await;
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
                //info!("Sending vote: {:?}", own_header);*/
            }

            //info!("VOTES: {}", self.votes.len());
        }
        if self.votes.len() >= self.header_size {
            //for vote in &self.votes {
                //info!("{} sending vote {:?}", self.name, vote);
            //}
            // broadcast votes
            let own_header = Header::new(self.name, self.votes.drain(..).collect(), &mut self.signature_service).await;
            let bytes = bincode::serialize(&PrimaryMessage::Header(own_header.clone()))
                .expect("Failed to serialize our own header");
            let _handlers = self.network.broadcast(self.addresses.clone(), Bytes::from(bytes)).await;
        }
        
        Ok(())
    }

    // Main loop listening to incoming messages.
    pub async fn run(&mut self) {
        let timer: tokio::time::Sleep = sleep(Duration::from_millis(TIMER));
        tokio::pin!(timer);

        loop {
            let result = tokio::select! {
                // We receive here messages from other primaries.
                Some(message) = self.rx_primaries.recv() => {
                    match message {
                        PrimaryMessage::Header(header) => self.process_header(&header, &mut timer).await,
                        _ => panic!("Unexpected core message")
                    }
                },

                () = &mut timer => {
                    //info!("Votes of {}: {}", self.name, self.votes.len());

                    if self.votes.len() > 0 {
                        //for vote in &self.votes {
                            //info!("{} sending vote {:?}", self.name, vote);
                        //}
                        // broadcast votes
                        //info!("{} sending header with {} votes", self.name, self.votes.len());
                        let own_header = Header::new(self.name, self.votes.drain(..).collect(), &mut self.signature_service).await;
                        let bytes = bincode::serialize(&PrimaryMessage::Header(own_header.clone()))
                            .expect("Failed to serialize our own header");
                        let _handlers = self.network.broadcast(self.addresses.clone(), Bytes::from(bytes)).await;
                    }

                    /*if self.decided.len() > 0 {
                        //for election_id in &self.decided {
                             //#[cfg(feature = "benchmark")]
                            // NOTE: This log entry is used to compute performance.
                            info!("Committed {} -> {:?}", self.decided.len(), self.counter);

                            self.counter += 1;
                        //}
                        self.decided = BTreeSet::new();
                    }*/

                    let deadline = Instant::now() + Duration::from_millis(TIMER);
                    timer.as_mut().reset(deadline);
                    
                    Ok(())
                }

                // We also receive here our new headers created by the `Proposer`.
                Some(header) = self.rx_proposer.recv() => self.process_header(&header, &mut timer).await,
            };
            match result {
                Ok(()) => (),
                Err(e @ DagError::TooOld(..)) => debug!("{}", e),
                Err(e) => warn!("{}", e),
            }
        }
    }
}

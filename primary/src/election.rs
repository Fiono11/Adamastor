use std::{collections::{BTreeSet, HashMap}, sync::{Arc, Mutex, Condvar}, thread::{self, sleep}, time::Duration};
use crypto::{PublicKey as PublicAddress, Digest};

use crate::{Round, Header, constants::{QUORUM, SEMI_QUORUM}, core::TxHash, messages::Vote};

pub type ElectionId = Digest;

#[derive(Debug, Clone)]
pub struct Election {
    pub tallies: HashMap<Round, Tally>,
    pub decided: bool,
    pub commit: Option<Digest>,
    pub highest: Option<Digest>,
    pub proof_round: Option<Round>,
    pub highest_round: Option<Round>,
}

impl Election {
    pub fn new() -> Self {
        let mut tallies = HashMap::new();
        tallies.insert(0, Tally::new());
        Self {
            tallies,
            decided: false,
            commit: None,
            highest: None,
            proof_round: None,
            highest_round: None,
        }
    }

    pub fn insert_vote(&mut self, vote: &Vote, author: PublicAddress) {
        let tx_hash = vote.tx_hash.clone();
        let round = vote.consensus_round;
        if !vote.commit {
            if let Some(highest) = self.highest.clone() {
                if tx_hash > highest {
                    self.highest = Some(tx_hash.clone());
                }
            }
            else {
                self.highest = Some(tx_hash.clone());
            }

            if let Some(hr) = self.highest_round {
                if round > hr {
                    self.highest_round = Some(round);
                }
            }
            else {
                self.highest_round = Some(round);
            }
        }

        match self.tallies.get_mut(&vote.election_round) {
            Some(tally) => {
                tally.insert_to_tally(tx_hash, author, vote.commit, vote.consensus_round);
            }
            None => {
                let mut tally = Tally::new();
                Tally::insert_to_tally(&mut tally, tx_hash.clone(), author, vote.commit, vote.consensus_round);
                self.tallies.insert(vote.election_round, tally);
            }
        }
    }

    pub fn voted_or_committed(&self, pa: &PublicAddress, round: Round) -> bool {
        match self.tallies.get(&round) {
            Some(tally) => {
                for vote_set in tally.votes.values().chain(tally.commits.values()) {
                    if vote_set.contains(pa) {
                        return true;
                    }
                }
            }
            None => return false,
        }
        false
    }    
}

#[derive(Debug, Clone)]
pub struct Tally {
    pub votes: HashMap<(TxHash, Round), BTreeSet<PublicAddress>>,
    pub commits: HashMap<(TxHash, Round), BTreeSet<PublicAddress>>,
    pub timer: Arc<(Mutex<Timer>, Condvar)>,
}

impl Tally {
    pub fn new() -> Self {
        let timer = Arc::new((Mutex::new(Timer::Active), Condvar::new())).clone();
        let timer_clone = Arc::clone(&timer);
        thread::spawn(move || {
            sleep(Duration::from_millis(ROUND_TIMER as u64));
            //debug!("round {} of {:?} expired!", round, id);
            let &(ref mutex, ref cvar) = &*timer_clone;
            let mut value = mutex.lock().unwrap();
            *value = Timer::Expired;
            cvar.notify_one();
        });

        Self {
            votes: HashMap::new(),
            commits: HashMap::new(),
            timer,
        }
    }

    pub fn find_quorum_of_votes(&self) -> Option<(&TxHash, &Round)> {
        for ((tx_hash, round), vote_set) in &self.votes {
            if vote_set.len() >= QUORUM {
                return Some((tx_hash, round));
            }
        }
        None
    }

    pub fn find_quorum_of_commits(&self) -> Option<(&TxHash, &Round)> {
        for ((tx_hash, round), commit_set) in &self.commits {
            if commit_set.len() >= QUORUM {
                return Some((tx_hash, round));
            }
        }
        None
    }

    pub fn total_votes(&self) -> usize {
        self.votes.values().map(|vote_set| vote_set.len()).sum()
    }

    fn insert_to_tally(&mut self, tx_hash: Digest, author: PublicAddress, is_commit: bool, round: Round) {
        let target = if is_commit { &mut self.commits } else { &mut self.votes };
        match target.get_mut(&(tx_hash.clone(), round)) {
            Some(btreeset) => {
                btreeset.insert(author);
            }
            None => {
                let mut btreeset = BTreeSet::new();
                btreeset.insert(author);
                target.insert((tx_hash, round), btreeset);
            }
        }
    }
}

pub const ROUND_TIMER: usize = 0;

#[derive(Debug, Clone, PartialEq)]
pub enum Timer {
    Active,
    Expired,
}
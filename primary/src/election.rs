use std::{collections::{BTreeSet, HashMap}, sync::{Arc, Mutex, Condvar}, thread::{self, sleep}, time::Duration};
use crypto::{PublicKey as PublicAddress, Digest};

use crate::{Round, Header, constants::{QUORUM, SEMI_QUORUM}, proposer::TxHash, messages::Vote};

pub type ElectionId = Digest;

#[derive(Debug, Clone)]
pub struct Election {
    //pub round: Round,
    pub tallies: HashMap<Round, Tally>,
    pub decided: bool,
    pub commit: Option<Digest>,
    pub highest: Option<Digest>,
    pub proof_round: Option<Round>,
    //pub voted: bool,
    //pub committed: bool,
}

impl Election {
    pub fn new() -> Self {
        let mut tallies = HashMap::new();
        tallies.insert(0, Tally::new());
        Self {
            //round: 0,
            tallies,
            decided: false,
            commit: None,
            highest: None,
            proof_round: None,
            //voted: false,
            //committed: false,
        }
    }

    pub fn insert_vote(&mut self, vote: &Vote, author: PublicAddress) {
        let tx_hash = vote.tx_hash.clone();
        if !vote.commit {
            if let Some(highest) = self.highest.clone() {
                if tx_hash > highest {
                    self.highest = Some(tx_hash.clone());
                }
            }
            else {
                self.highest = Some(tx_hash.clone());
            }
        }

        match self.tallies.get_mut(&vote.round) {
            Some(tally) => {
                tally.insert_to_tally(tx_hash, author, vote.commit);
            }
            None => {
                let mut tally = Tally::new();
                Tally::insert_to_tally(&mut tally, tx_hash.clone(), author, vote.commit);
                self.tallies.insert(vote.round, tally);
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
    pub votes: HashMap<TxHash, BTreeSet<PublicAddress>>,
    pub commits: HashMap<TxHash, BTreeSet<PublicAddress>>,
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

    pub fn find_quorum_of_votes(&self) -> Option<&TxHash> {
        for (tx_hash, vote_set) in &self.votes {
            if vote_set.len() >= QUORUM {
                return Some(tx_hash);
            }
        }
        None
    }

    pub fn find_quorum_of_commits(&self) -> Option<&TxHash> {
        for (tx_hash, commit_set) in &self.commits {
            if commit_set.len() >= QUORUM {
                return Some(tx_hash);
            }
        }
        None
    }

    pub fn total_votes(&self) -> usize {
        self.votes.values().map(|vote_set| vote_set.len()).sum()
    }

    fn insert_to_tally(&mut self, tx_hash: Digest, author: PublicAddress, is_commit: bool) {
        let target = if is_commit { &mut self.commits } else { &mut self.votes };
        match target.get_mut(&tx_hash) {
            Some(btreeset) => {
                btreeset.insert(author);
            }
            None => {
                let mut btreeset = BTreeSet::new();
                btreeset.insert(author);
                target.insert(tx_hash, btreeset);
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
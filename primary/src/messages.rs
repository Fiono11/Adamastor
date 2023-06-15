use crate::core::TxHash;
use crate::election::ElectionId;
// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::{DagError, DagResult};
use crate::primary::Round;
use config::{Committee, WorkerId};
use ed25519_dalek::{Digest as _, Sha512};
use crypto::{Digest, PublicKey as PublicAddress, Signature, SignatureService};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::convert::{TryInto, TryFrom};
use std::fmt;

/// This trait is implemented by all messages that can be hashed.
pub trait Hash {
    fn digest(&self) -> TxHash;
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Header {
    pub author: PublicAddress,
    pub votes: BTreeSet<Vote>,
    pub signature: Signature,
    //pub id: Digest,
}

impl Header {
    pub async fn new(
        author: PublicAddress,
        votes: BTreeSet<Vote>,
        signature_service: &mut SignatureService,
    ) -> Self {
        // Ensure all votes in the same round with the same election_id are unique.
        let mut seen_ids = std::collections::HashMap::new();
        for vote in &votes {
            match seen_ids.insert((vote.round, vote.election_id.clone()), true) {
                None => (), // Key was not present in the map, everything is fine.
                Some(_) => panic!("Votes in the same round cannot have the same election_id!"),
            }
        }

        let header = Self {
            author,
            votes: BTreeSet::new(),
            signature: Signature::default(),
            //id: Digest::default(),
        };
        let id = header.digest();
        let signature = signature_service.request_signature(Digest::default()).await;
        Self {
            //id,
            votes,
            signature,
            ..header
        }
    }

    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        // Ensure the header id is well formed.
        //ensure!(self.digest() == self.id, DagError::InvalidHeaderId);

        // Ensure the authority has voting rights.
        let voting_rights = committee.stake(&self.author);
        ensure!(voting_rights > 0, DagError::UnknownAuthority(self.author.clone()));

        // Ensure all worker ids are correct.
        /*for worker_id in self.payload.values() {
            committee
                .worker(&PK(self.author.to_bytes()), &worker_id)
                .map_err(|_| DagError::MalformedHeader(self.id.clone()))?;
        }*/

        // Check the signature.
        self.signature
            .verify(&Digest::default(), &self.author)
            .map_err(DagError::from)
    }
}

impl Hash for Header {
    fn digest(&self) -> TxHash {
        let mut hasher = Sha512::new();
        hasher.update(self.author);
        //hasher.update(self.round.to_le_bytes());
        for vote in &self.votes {
            hasher.update(vote.digest());
        }
        //for x in &self.parents {
            //hasher.update(x);
        //}
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

/*impl fmt::Debug for Header {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: B{}({}, {})",
            self.payload.1,
            self.round,
            self.author,
            self.payload.0,
        )
    }
}

impl fmt::Display for Header {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "B{}({})", self.round, self.id)
    }
}*/

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Vote {
    pub round: Round,
    pub tx_hash: TxHash,
    pub election_id: ElectionId,
    pub commit: bool,
}

impl Vote {
    pub async fn new(
        round: Round,
        tx_hash: TxHash,
        election_id: ElectionId,
        commit: bool,
    ) -> Self {
        Self {
            round, tx_hash, election_id, commit,
        }
    }
}

impl Hash for Vote {
    fn digest(&self) -> TxHash {
        let mut hasher = Sha512::new();
        hasher.update(self.round.to_le_bytes());
        hasher.update(&self.tx_hash);
        hasher.update(&self.election_id);
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for Vote {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: V{}({}, {})",
            self.digest(),
            self.round,
            self.tx_hash,
            self.election_id,
        )
    }
}

impl fmt::Display for Vote {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "B{}({})", self.round, self.digest())
    }
}


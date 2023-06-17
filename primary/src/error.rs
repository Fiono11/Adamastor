// Copyright(C) Facebook, Inc. and its affiliates.
use crate::primary::Round;
use crypto::{CryptoError, Digest as TxHash, PublicKey as PublicAddress};
use store::StoreError;
use thiserror::Error;

#[macro_export]
macro_rules! bail {
    ($e:expr) => {
        return Err($e);
    };
}

#[macro_export(local_inner_macros)]
macro_rules! ensure {
    ($cond:expr, $e:expr) => {
        if !($cond) {
            bail!($e);
        }
    };
}

pub type DagResult<T> = Result<T, DagError>;

#[derive(Debug, Error)]
pub enum DagError {
    #[error("Invalid signature")]
    InvalidSignature(#[from] CryptoError),

    #[error("Storage failure: {0}")]
    StoreError(#[from] StoreError),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] Box<bincode::ErrorKind>),

    #[error("Invalid header id")]
    InvalidHeaderId,

    #[error("Malformed header {0}")]
    MalformedHeader(TxHash),

    #[error("Received message from unknown authority {0}")]
    UnknownAuthority(PublicAddress),

    #[error("Authority {0} appears in quorum more than once")]
    AuthorityReuse(PublicAddress),

    #[error("Received unexpected vote fo header {0}")]
    UnexpectedVote(TxHash),

    #[error("Received certificate without a quorum")]
    CertificateRequiresQuorum,

    #[error("Parents of header {0} are not a quorum")]
    HeaderRequiresQuorum(TxHash),

    #[error("Message {0} (round {1}) too old")]
    TooOld(TxHash, Round),
}

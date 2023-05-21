// Copyright (c) 2018-2022 The MobileCoin Foundation

//! Errors that can occur when creating a new TxOut

use alloc::{string::String};
use core::str::Utf8Error;

use displaydoc::Display;
use mc_crypto_keys::KeyError;
use mc_transaction_types::AmountError;
use serde::{Deserialize, Serialize};

/// An error that occurs when creating a new TxOut
#[derive(Clone, Debug, Display)]
pub enum NewTxError {
    /// Amount: {0}
    Amount(AmountError),
}

impl From<AmountError> for NewTxError {
    fn from(src: AmountError) -> NewTxError {
        NewTxError::Amount(src)
    }
}

/// An error that occurs when handling a TxOut
#[derive(Clone, Debug, Display, Ord, PartialOrd, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub enum TxOutConversionError {
    /// Unknown Masked Amount Version
    UnknownMaskedAmountVersion,
}

/// An error that occurs when view key matching a TxOut
#[derive(Clone, Debug, Display)]
pub enum ViewKeyMatchError {
    /// Key: {0}
    Key(KeyError),
    /// Amount: {0}
    Amount(AmountError),
    /// Unknown Masked Amount Version
    UnknownMaskedAmountVersion,
}

impl From<KeyError> for ViewKeyMatchError {
    fn from(src: KeyError) -> Self {
        Self::Key(src)
    }
}

impl From<AmountError> for ViewKeyMatchError {
    fn from(src: AmountError) -> Self {
        Self::Amount(src)
    }
}

/// An error that occurs when creating a new Memo for a TxOut
///
/// These errors are usually created by a MemoBuilder.
/// We have included error codes for some known useful error conditions.
/// For a custom MemoBuilder, you can try to reuse those, or use the Other
/// error code.
#[derive(Clone, Debug, Display, Eq, PartialEq)]
pub enum NewMemoError {
    /// Limits for '{0}' value exceeded
    LimitsExceeded(&'static str),
    /// Multiple change outputs not supported
    MultipleChangeOutputs,
    /// Creating more outputs after the change output is not supported
    OutputsAfterChange,
    /// Changing the fee after the change output is not supported
    FeeAfterChange,
    /// Invalid recipient address
    InvalidRecipient,
    /// Multiple outputs are not supported
    MultipleOutputs,
    /// Missing output
    MissingOutput,
    /// Missing required input to build the memo: {0}
    MissingInput(String),
    /// Mixed Token Ids are not supported in these memos
    MixedTokenIds,
    /// Destination memo is not supported
    DestinationMemoNotAllowed,
    /// Improperly configured input: {0}
    BadInputs(String),
    /// Utf-8 did not properly decode
    Utf8Decoding,
    /// Attempted value: {1} > Max Value: {0}
    MaxFeeExceeded(u64, u64),
    /// Payment request and intent ID both are set
    RequestAndIntentIdSet,
    /// Defragmentation transaction with non-zero change
    DefragWithChange,
    /// Other: {0}
    Other(String),
}

impl From<Utf8Error> for NewMemoError {
    fn from(_: Utf8Error) -> Self {
        Self::Utf8Decoding
    }
}

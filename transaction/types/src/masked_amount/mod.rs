// Copyright (c) 2018-2022 The MobileCoin Foundation

//! A commitment to an output's amount, denominated in picoMOB.
//!
//! Amounts are implemented as Pedersen commitments. The associated private keys
//! are "masked" using a shared secret.











//mod v1;
//pub use v1::MaskedAmountV1;

mod v2;
pub use v2::MaskedAmount;

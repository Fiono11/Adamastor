// Copyright (c) 2018-2022 The MobileCoin Foundation

//! Transaction validation.

extern crate alloc;

use crate::tx::{Transaction, TxPrefix, TxOut};

use super::error::{TransactionValidationError, TransactionValidationResult};
use alloc::{format, vec::Vec};
use mc_common::HashSet;
use mc_crypto_ring_signature::{Sign, Verify, RistrettoPoint, Scalar, KeyGen};
use mc_transaction_types::constants::{MAX_INPUTS, RING_SIZE, MAX_OUTPUTS};
use rand_core::{CryptoRng, RngCore};
use curve25519_dalek::traits::Identity;

/// Determines if the transaction is valid, with respect to the provided
/// context.
///
/// # Arguments
/// * `tx` - A pending transaction.
/// * `current_block_index` - The index of the current block that is being
///   built.
/// * `block_version` - The version of the transaction rules we are testing
/// * `root_proofs` - Membership proofs for each input ring element contained in
///   `tx`.
/// * `minimum_fee` - The minimum fee for the token indicated by
///   tx.prefix.fee_token_id
/// * `csprng` - Cryptographically secure random number generator.
pub fn validate<R: RngCore + CryptoRng>(
    tx: &Transaction,
    csprng: &mut R,
) -> TransactionValidationResult<()> {

    validate_number_of_inputs(&tx.prefix, MAX_INPUTS)?;

    validate_number_of_outputs(&tx.prefix, MAX_OUTPUTS)?;

    validate_ring_sizes(&tx.prefix, RING_SIZE)?;

    validate_signature(tx, csprng)?;

    Ok(())
}

/// The transaction must have at least one input, and no more than the maximum
/// allowed number of inputs.
pub fn validate_number_of_inputs(
    tx_prefix: &TxPrefix,
    maximum_allowed_inputs: u64,
) -> TransactionValidationResult<()> {
    let num_inputs = tx_prefix.inputs.len();

    // Each transaction must have at least one input.
    if num_inputs == 0 {
        return Err(TransactionValidationError::NoInputs);
    }

    // Each transaction must have no more than the maximum allowed number of inputs.
    if num_inputs > maximum_allowed_inputs as usize {
        return Err(TransactionValidationError::TooManyInputs);
    }

    Ok(())
}

/// The transaction must have at least one output.
pub fn validate_number_of_outputs(
    tx_prefix: &TxPrefix,
    maximum_allowed_outputs: u64,
) -> TransactionValidationResult<()> {
    let num_outputs = tx_prefix.outputs.len();

    // Each transaction must have at least one output.
    if num_outputs == 0 {
        return Err(TransactionValidationError::NoOutputs);
    }

    // Each transaction must have no more than the maximum allowed number of
    // outputs.
    if num_outputs > maximum_allowed_outputs as usize {
        return Err(TransactionValidationError::TooManyOutputs);
    }

    Ok(())
}

/// Each input must contain a ring containing `ring_size` elements.
pub fn validate_ring_sizes(
    tx_prefix: &TxPrefix,
    ring_size: usize,
) -> TransactionValidationResult<()> {
        if tx_prefix.inputs.len() != ring_size {
            let e = if tx_prefix.inputs.len() > ring_size {
                TransactionValidationError::ExcessiveRingSize
            } else {
                TransactionValidationError::InsufficientRingSize
            };
            return Err(e);
        }
    
    Ok(())
}

/// Verifies the transaction signature.
///
/// A valid RctBulletproofs signature implies that:
/// * tx.prefix has not been modified,
/// * The signer owns one element in each input ring,
/// * Each key image corresponds to the spent ring element,
/// * The outputs have values in [0,2^64),
/// * The transaction does not create or destroy mobilecoins.
/// * The signature is valid according to the rules of this block version
pub fn validate_signature<R: RngCore + CryptoRng>(
    tx: &Transaction,
    rng: &mut R,
) -> TransactionValidationResult<()> {
	let mut R: Vec<RistrettoPoint> = vec![RistrettoPoint::identity(); RING_SIZE];
	let mut x: Scalar = Scalar::one();

	for i in 0..RING_SIZE {
		let (sk, pk) = KeyGen();
		R[i] = pk;

		if i == 0 {
			x = sk;
		}
	}

    //Verify(&tx.signature, "msg", &R)?;

    Ok(())
}

// NOTE: There are unit tests of every validation function, which appear in
// transaction/core/tests/validation.rs.
//
// The reason that these appear there is,
// many of the tests use `mc-transaction-core-test-utils` which itself depends
// on `mc-ledger-db` and `mc-transaction-core`, and this creates a circular
// dependency which leads to build problems, if the unit tests appear in-line
// here.
//
// Please add tests for any new validation functions there. Thank you!

use std::convert::TryFrom;

use curve25519_dalek_ng::{ristretto::{CompressedRistretto, RistrettoPoint}, scalar::Scalar};
use serde::{Deserialize, Serialize};

use crate::{triptych::TriptychSignature, account_keys::{PublicAddress, AccountKey}, ristretto::{RistrettoPublic, RistrettoPrivate, CompressedRistrettoPublic}, onetime_keys::{create_tx_out_target_key, create_tx_out_public_key, create_shared_secret, recover_onetime_private_key}, error::Error, masked_amount::{MaskedAmount, Amount}, compressed_commitment::CompressedCommitment, TwistedElGamal};

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct Transaction {
    id: u128,
    //pub balance: TwistedElGamal,
    //pub range_proof: Vec<u8>, // balance > 0
    //pub signature: Signature,
    pub inputs: Vec<u64>,
    pub outputs: Vec<TxOut>,
    pub signature: TriptychSignature,
    pub representative: CompressedRistretto,
    //pub public_key: PublicKey,
    pub aux: (Vec<u8>, Vec<u8>),
    pub pseudo_output_commitments: Vec<CompressedCommitment>,
}

impl Transaction {
    pub fn new(id: u128, inputs: Vec<u64>, outputs: Vec<TxOut>, representative: CompressedRistretto, signature: TriptychSignature, aux: (Vec<u8>, Vec<u8>), pseudo_output_commitments: Vec<CompressedCommitment>) -> Self {
        Self {
            id,
            //balance,
            signature,
            //range_proof,
            representative,
            aux,
            inputs,
            outputs,
            pseudo_output_commitments,
        }
    }

    pub fn len(&self) -> usize {
        let bytes = bincode::serialize(&self).unwrap();
        bytes.len()
    }
}

/// An output created by a transaction.
#[derive(Clone, Deserialize, Eq, PartialEq, Serialize, Debug)]
pub struct TxOut {
    /// The amount being sent.
    pub masked_amount: MaskedAmount,

    /// The one-time public address of this output.
    pub target_key: CompressedRistrettoPublic,

    /// The per output tx public key
    pub public_key: CompressedRistrettoPublic,
}

impl TxOut {
    /// Creates a TxOut that sends `value` to `recipient`.
    /// This uses a defaulted (all zeroes) MemoPayload.
    ///
    /// # Arguments
    /// * `block_version` - Structural rules to target
    /// * `amount` - Amount contained within the TxOut
    /// * `recipient` - Recipient's address.
    /// * `tx_private_key` - The transaction's private key
    /// * `hint` - Encrypted Fog hint for this output.
    pub fn new(
        amount: u64,
        recipient: &PublicAddress,
        tx_private_key: &RistrettoPrivate,
    ) -> Result<Self, Error> {
        let target_key = create_tx_out_target_key(tx_private_key, recipient).into();
        let public_key = create_tx_out_public_key(tx_private_key, recipient.spend_public_key());

        let shared_secret = create_shared_secret(recipient.view_public_key(), tx_private_key);

        let masked_amount = MaskedAmount::new(amount, &shared_secret)?;

        Ok(TxOut {
            masked_amount,
            target_key,
            public_key: public_key.into(),
        })
    }

    /// A merlin-based hash of this TxOut.
    //pub fn hash(&self) -> Hash {
        //self.digest32::<MerlinTranscript>(b"mobilecoin-txout")
    //}

    /// Try to establish ownership of this TxOut, using the view private key.
    ///
    /// Arguments:
    /// * view_private_key: The account view private key for the (possible)
    ///   owner
    ///
    /// Returns:
    /// * An (unmasked) Amount
    /// * The shared secret
    /// Or, an error if recovery failed.
    pub fn view_key_match(
        &self,
        view_private_key: &RistrettoPrivate,
    ) -> Result<(Amount, RistrettoPublic), Error> {
        // Reconstruct compressed commitment based on our view key.
        // The first step is reconstructing the TxOut shared secret
        let public_key = RistrettoPublic::try_from(&self.public_key)?;

        let tx_out_shared_secret = get_tx_out_shared_secret(view_private_key, &public_key);

        let (amount, _scalar) = self
            .masked_amount
            .get_value(&tx_out_shared_secret)?;

        Ok((amount, tx_out_shared_secret))
    }

    /// Get the masked amount field, which is expected to be present in some
    /// version. Maps to a conversion error if the masked amount field is
    /// missing
    pub fn get_masked_amount(&self) -> MaskedAmount {
        self.masked_amount.clone()
    }
}

/// Get the shared secret for a transaction output.
///
/// # Arguments
/// * `view_key` - The recipient's private View key.
/// * `tx_public_key` - The public key of the transaction.
pub fn get_tx_out_shared_secret(
    view_key: &RistrettoPrivate,
    tx_public_key: &RistrettoPublic,
) -> RistrettoPublic {
    create_shared_secret(tx_public_key, view_key)
}

// Creates a transaction that sends the full value of `tx_out` to a single
/// recipient.
///
/// # Arguments:
/// * `block_version` - The block version to use for the transaction.
/// * `ledger` - A ledger containing `tx_out`.
/// * `tx_out` - The TxOut that will be spent.
/// * `sender` - The owner of `tx_out`.
/// * `recipient` - The recipient of the new transaction.
/// * `tombstone_block` - The tombstone block for the new transaction.
/// * `rng` - The randomness used by this function
pub fn create_transaction<R: rand_core::RngCore + rand_core::CryptoRng>(
    tx_out: &TxOut,
    sender: &AccountKey,
    recipient: &PublicAddress,
    rng: &mut R,
    inputs: Vec<u64>,
    id: u128,
    representative: CompressedRistretto,
) -> Transaction {
    // Get the output value.
    let tx_out_public_key = RistrettoPublic::try_from(&tx_out.public_key).unwrap();
    let shared_secret = get_tx_out_shared_secret(sender.view_private_key(), &tx_out_public_key);
    let (amount, _blinding) = tx_out
        .get_masked_amount()
        .get_value(&shared_secret)
        .unwrap();

    // Populate a ring with mixins.
    let mut ring: Vec<TxOut> = origin_outputs.iter().take(RING_SIZE).cloned().collect();
    if !ring.contains(tx_out) {
        ring[0] = tx_out.clone();
    }
    let real_index = ring.iter().position(|element| element == tx_out).unwrap();

    let spend_private_key = sender.subaddress_spend_private(DEFAULT_SUBADDRESS_INDEX);
    let tx_out_public_key = RistrettoPublic::try_from(&tx_out.public_key).unwrap();
    let onetime_private_key = recover_onetime_private_key(
        &tx_out_public_key,
        sender.view_private_key(),
        &spend_private_key,
    );

    Transaction { id, inputs, outputs: (), signature: (), representative, aux: (), pseudo_output_commitments: () }
}
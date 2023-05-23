// Copyright (c) 2018-2022 The MobileCoin Foundation

//! Definition of a MobileCoin transaction and a MobileCoin TxOut

use std::{fmt};
use chacha20poly1305::{ChaCha20Poly1305, Key, KeyInit, AeadCore, aead::Aead, Nonce};
use curve25519_dalek::{traits::Identity, constants::RISTRETTO_BASEPOINT_POINT, ristretto::CompressedRistretto};
use mc_account_keys::{PublicAddress, AccountKey};
use mc_crypto_digestible::{Digestible, MerlinTranscript};
use mc_crypto_keys::{tx_hash::TxHash, CompressedRistrettoPublic, RistrettoPublic, RistrettoPrivate};
use mc_crypto_ring_signature::{KeyImage, get_tx_out_shared_secret, onetime_keys::{create_shared_secret, create_tx_out_public_key, create_tx_out_target_key}, ReducedTxOut, CompressedCommitment, TriptychSignature, Sign, Scalar, KeyGen, RistrettoPoint};
use mc_transaction_types::{MaskedAmount, Amount, constants::RING_SIZE};
use mc_util_from_random::FromRandom;
use rand_core::{OsRng};
use serde::{Deserialize, Serialize};
use prost::Message;
use zeroize::Zeroize;
use mc_common::Hash;
use crate::range_proofs::{generate_range_proof};
use crate::tx_error::{TxOutConversionError, ViewKeyMatchError, NewTxError};
use bulletproofs_og::PedersenGens;

/// A CryptoNote-style transaction.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, Digestible, Debug)]
pub struct Transaction {
    /// The transaction contents.
    //#[prost(message, required, tag = "1")]
    pub prefix: TxPrefix,

    /// The transaction signature.
    //#[prost(message, required, tag = "2")]
    pub signature: TriptychSignature,

    pub range_proof_bytes: Vec<u8>,

    pub commitment: CompressedRistretto,

    /// The transaction id.
    //#[prost(message, repeated, tag = "3")]
    pub id: Vec<u8>,
}

impl fmt::Display for Transaction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.tx_hash())
    }
}

impl Transaction {
    /// Compute a 32-byte hash from all of the contents of a Tx
    pub fn tx_hash(&self) -> TxHash {
        TxHash::from(self.digest32::<MerlinTranscript>(b"mobilecoin-tx"))
    }

    /// Key image "spent" by this transaction.
    pub fn key_images(&self) -> KeyImage {
        self.signature.key_image
    }
    
    /// Output public keys contained in this transaction.
    pub fn output_public_keys(&self) -> Vec<CompressedRistrettoPublic> {
        self.prefix
            .outputs
            .iter()
            .map(|tx_out| tx_out.aux)
            .collect()
    }

    pub fn len(&self) -> usize {
        let bytes = bincode::serialize(&self).unwrap();
        bytes.len()
    }
}

/// TxPrefix is the Tx struct without the signature.  It is used to
/// calculate the prefix hash for signing and verifying.
///
/// Note: If you add something here, consider if it should be added to the
/// TxSummary also for hardware wallet visibility.
#[derive(Clone, Deserialize, Eq, PartialEq, Serialize, Message, Digestible)]
pub struct TxPrefix {
    /// List of inputs to the transaction.
    #[prost(message, repeated, tag = "1")]
    pub inputs: Vec<u64>,

    /// List of outputs from the transaction.
    #[prost(message, repeated, tag = "2")]
    pub outputs: Vec<TxOut>,
}

impl TxPrefix {
    /// Create a new TxPrefix.
    ///
    /// # Arguments:
    /// * `inputs` - Inputs spent by the transaction.
    /// * `outputs` - Outputs created by the transaction.
    pub fn new(
        inputs: Vec<u64>,
        outputs: Vec<TxOut>,
    ) -> TxPrefix {
        TxPrefix {
            inputs,
            outputs,
        }
    }

    /// Digestible-crate hash of `self` using Merlin
    pub fn hash(&self) -> TxHash {
        TxHash::from(self.digest32::<MerlinTranscript>(b"mobilecoin-tx-prefix"))
    }

    /// Get all output commitments.
    pub fn output_commitments(&self) -> Vec<CompressedCommitment> {
        self.outputs
            .iter()
            .map(|output| output.get_masked_amount().commitment)
            .collect()
    }
}

/// An "input" to a transaction.
#[derive(Clone, Deserialize, Digestible, Eq, PartialEq, Message, Serialize, Zeroize)]
pub struct TxIn {
    /// A "ring" of outputs containing the single output that is being spent.
    /// It would be nice to use [TxOut; RING_SIZE] here, but Prost only works
    /// with Vec.
    #[prost(message, repeated, tag = "1")]
    pub ring: Vec<u64>,
}

/// An output created by a transaction.
#[derive(Clone, Deserialize, Digestible, Eq, Hash, Message, PartialEq, Serialize, Zeroize)]
pub struct TxOut {
    /// The amount being sent.
    #[prost(message, required, tag = "1")]
    pub masked_amount: MaskedAmount,

    /// The one-time public address of this output.
    #[prost(message, required, tag = "2")]
    pub target_key: CompressedRistrettoPublic,

    /// Auxilliary information for the representative to get the two-way shared secret to decrypt the cipher
    #[prost(message, required, tag = "3")]
    pub aux: CompressedRistrettoPublic,

    /// Cipher that encrypts the three-way shared secret that opens the output commitment
    #[prost(bytes, tag = "4")]
    pub cipher_receiver: Vec<u8>,

    /// Cipher that encrypts the three-way shared secret that opens the output commitment
    #[prost(bytes, tag = "5")]
    pub cipher_representative: Vec<u8>,
}

impl TxOut {
    /// Creates a TxOut that sends `value` to `recipient`, with a custom memo
    /// attached. The memo is produced by a callback function which is
    /// passed the value and tx_public_key.
    ///
    /// # Arguments
    /// * `block_version` - Structural rules to target
    /// * `amount` - Amount contained within the TxOut
    /// * `recipient` - Recipient's address.
    /// * `tx_private_key` - The transaction's private key
    ///   MemoPayload, or a NewMemo error
    pub fn new(
        amount: Amount,
        recipient: &PublicAddress,
        tx_private_key: &RistrettoPrivate,
        representative: PublicAddress,
        shared_secret: Scalar,
    ) -> Result<Self, NewTxError> {
        let (tx_target_key, tx_public_key) =
                get_output_public_keys(&tx_private_key, &recipient);

        let shared_secret1 = create_shared_secret(recipient.view_public_key(), tx_private_key);
        let shared_secret2 = create_shared_secret(&representative.view_public_key(), tx_private_key);

        //let shared_secret = Scalar::random(&mut rand_core::OsRng);

        let a_b_bytes = shared_secret1.0.compress();
        let key1 = Key::from_slice(a_b_bytes.as_bytes());
        let cipher1 = ChaCha20Poly1305::new(&key1);
        let ciphertext1 = cipher1.encrypt(&Nonce::default(), shared_secret.to_bytes().as_ref()).unwrap();

        let a_c_bytes = shared_secret2.0.compress();
        let key2 = Key::from_slice(a_c_bytes.as_bytes());
        let cipher2 = ChaCha20Poly1305::new(&key2);
        let nonce2 = ChaCha20Poly1305::generate_nonce(&mut OsRng); // 96-bits; unique per message
        let ciphertext2 = cipher2.encrypt(&nonce2, shared_secret.to_bytes().as_ref()).unwrap();
        
        let ss = shared_secret * RISTRETTO_BASEPOINT_POINT;

        let masked_amount = MaskedAmount::new(amount, &RistrettoPublic(ss)).unwrap();

        //let amount_shared_secret = Self::compute_amount_shared_secret(ss);
        //get_blinding_factors(amount_shared_secret);

        Ok(TxOut {
            masked_amount,
            target_key: tx_target_key.into(),
            aux: tx_public_key.into(),
            cipher_receiver: ciphertext2,
            cipher_representative: ciphertext1,
        })
    }

    /// A merlin-based hash of this TxOut.
    pub fn hash(&self) -> Hash {
        self.digest32::<MerlinTranscript>(b"mobilecoin-txout")
    }

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
    ) -> Result<(Amount, RistrettoPublic), ViewKeyMatchError> {
        // Reconstruct compressed commitment based on our view key.
        // The first step is reconstructing the TxOut shared secret
        let public_key = RistrettoPublic::try_from(&self.aux)?;

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

impl TryFrom<&TxOut> for ReducedTxOut {
    type Error = TxOutConversionError;
    fn try_from(src: &TxOut) -> Result<Self, Self::Error> {
        Ok(Self {
            public_key: src.aux,
            target_key: src.target_key,
            commitment: src.get_masked_amount().commitment,
        })
    }
}

/// Creates a transaction that sends the full value of `tx_out` to a single
/// recipient.
///
/// # Arguments:
/// * `tx_out` - The TxOut that will be spent.
/// * `sender` - The owner of `tx_out`.
/// * `recipient` - The recipient of the new transaction.
/// * `rng` - The randomness used by this function
pub fn create_transaction(
    _tx_out: &TxOut,
    _sender: &AccountKey,
    recipient: &PublicAddress,
    amount: u64,
    id: Vec<u8>, 
) -> Transaction {

    let mut inputs = Vec::new();

    for i in 0..RING_SIZE {
        inputs.push(i as u64);
    }

    let mut rng = rand_core::OsRng;
    let tx_private_key = RistrettoPrivate::from_random(&mut rng);

    let rep_account = AccountKey::default();

    let shared_secret = Scalar::random(&mut rng);

    let output = TxOut::new(Amount::new(amount), recipient, &tx_private_key, rep_account.to_public_address(), shared_secret).unwrap();

    let prefix = TxPrefix::new(inputs, vec![output]);

    let mut r: Vec<RistrettoPoint> = vec![RistrettoPoint::identity(); RING_SIZE];
    let mut x: Scalar = Scalar::one();

    for i in 0..RING_SIZE {
        let (sk, pk) = KeyGen();
        r[i] = pk;

        if i == 0 {
            x = sk;
        }
    }

    //let generator = generators();
    let generators: PedersenGens = PedersenGens::default();

    /*let mut message = [0u8; 32];
    rng.fill_bytes(&mut message);

    let mut ring: Vec<ReducedTxOut> = Vec::new();
    for _i in 0..RING_SIZE {
        let public_key = CompressedRistrettoPublic::from_random(&mut rng);
        let target_key = CompressedRistrettoPublic::from_random(&mut rng);
        let commitment = {
            let value = amount;
            let blinding = Scalar::random(&mut rng);
            CompressedCommitment::new(value, blinding, &generator)
        };
        ring.push(ReducedTxOut {
            public_key,
            target_key,
            commitment,
        });
    }

    // The real input.
    let onetime_private_key = RistrettoPrivate::from_random(&mut rng);

    let value = rng.next_u64();
    let blinding = Scalar::random(&mut rng);
    let commitment = CompressedCommitment::new(value, blinding, &generator);

    /*let reduced_tx_out = ReducedTxOut {
        target_key: CompressedRistrettoPublic::from(RistrettoPublic::from(
            &onetime_private_key,
        )),
        public_key: CompressedRistrettoPublic::from_random(&mut rng),
        commitment,
    };*/

    let reduced_tx_out = ReducedTxOut::try_from(tx_out).unwrap();

    //let real_index = rng.next_u64() as usize % (RING_SIZE + 1);
    let real_index = 0;
    ring.insert(real_index, reduced_tx_out);
    assert_eq!(ring.len(), RING_SIZE + 1);

    let output_blinding = Scalar::random(&mut rng);

    let signature = RingMLSAG::sign(&message, &ring, real_index, &onetime_private_key, value, &blinding, &output_blinding, &generator, &mut rng).unwrap();*/

    let (range_proof, commitment) = generate_range_proof(&amount, &shared_secret, &generators, &mut OsRng).unwrap();
    let range_proof_bytes = range_proof.to_bytes();

    let signature = Sign(&x, "msg", &r);

    Transaction { prefix, signature, range_proof_bytes, commitment, id }
}

// Get the account's i^th subaddress.
pub fn get_subaddress(
    account: &AccountKey,
    index: u64,
) -> (RistrettoPublic, RistrettoPublic, PublicAddress) {
    // (view, spend)
    let (c, d) = (
        account.subaddress_view_private(index),
        account.subaddress_spend_private(index),
    );

    // (View, Spend)
    let (c, d) = (RistrettoPublic::from(&c), RistrettoPublic::from(&d));
    // Look out! The argument ordering here is weird.
    let subaddress = PublicAddress::new(&d, &c);

    (c, d, subaddress)
}

// Returns (tx_target_key, tx_public_key)
fn get_output_public_keys(
    tx_private_key: &RistrettoPrivate,
    recipient: &PublicAddress,
) -> (RistrettoPublic, RistrettoPublic) {
    let tx_target_key = create_tx_out_target_key(tx_private_key, recipient);
    let tx_public_key = create_tx_out_public_key(tx_private_key, recipient.spend_public_key());
    (tx_target_key, tx_public_key)
}

#[cfg(test)]
mod tests {
    use curve25519_dalek::constants::RISTRETTO_BASEPOINT_POINT;
    use mc_account_keys::{
        AccountKey, PublicAddress,
    };
    use mc_crypto_keys::{RistrettoPrivate, RistrettoPublic};
    use mc_crypto_ring_signature::{get_tx_out_shared_secret, onetime_keys::create_shared_secret, Scalar};
    use mc_transaction_types::Amount;
    use mc_util_from_random::FromRandom;
    

    use super::TxOut;

    /*#[test]
    // Create a Tx, encode/decode it, and compare
    fn test_serialize_tx() {
        let mut rng = get_seeded_rng();
        for block_version in BlockVersion::iterator() {
            let recipient = PublicAddress::from_random(&mut rng);
            let amount = Amount::new(23, Mob::ID);
            let tx_private_key = RistrettoPrivate::from_random(&mut rng);
            let tx_out = TxOut::new(
                block_version,
                amount,
                &recipient,
                &tx_private_key,
                Default::default(),
            )
            .unwrap();

            // TxOut = decode(encode(TxOut))
            assert_eq!(tx_out, TxOut::decode(&tx_out.encode_to_vec()[..]).unwrap());

            let tx_in = TxIn {
                ring: vec![tx_out.clone()],
                proofs: vec![],
                input_rules: None,
            };

            // TxIn = decode(encode(TxIn))
            assert_eq!(tx_in, TxIn::decode(&tx_in.encode_to_vec()[..]).unwrap());

            let prefix = TxPrefix {
                inputs: vec![tx_in],
                outputs: vec![tx_out],
                fee: Mob::MINIMUM_FEE,
                fee_token_id: *Mob::ID,
                tombstone_block: 23,
            };

            assert_eq!(
                prefix,
                TxPrefix::decode(&prefix.encode_to_vec()[..]).unwrap()
            );

            // TODO: use a meaningful signature.
            let signature = SignatureRctBulletproofs::default();

            let tx = Transaction {
                prefix,
                signature,
                fee_map_digest: vec![],
            };

            let recovered_tx: Transaction = Transaction::decode(&tx.encode_to_vec()[..]).unwrap();
            assert_eq!(tx, recovered_tx);
        }
    }*/

    // round trip memos from `TxOut` constructors through `decrypt_memo()`
    #[test]
    fn test_decrypt_memo() {
        let mut rng = rand_core::OsRng;

        let bob = AccountKey::new(
            &RistrettoPrivate::from_random(&mut rng),
            &RistrettoPrivate::from_random(&mut rng),
        );
        //let bob_addr = bob.default_subaddress();
        let bob_addr = PublicAddress {
            view_public_key: RistrettoPublic::from(bob.view_private_key().0 * RISTRETTO_BASEPOINT_POINT),
            spend_public_key: RistrettoPublic::from(bob.spend_private_key().0 * RISTRETTO_BASEPOINT_POINT),
        };

        {
            let tx_private_key = RistrettoPrivate::from_random(&mut rng);

            let tx_out = TxOut::new(
                Amount {
                    value: 13,
                },
                &bob.change_subaddress(),
                &tx_private_key,
                PublicAddress::default(),
                Scalar::one(), // fix this
            )
            .unwrap();

            let _ss = get_tx_out_shared_secret(
                bob.view_private_key(),
                &RistrettoPublic::try_from(&tx_out.aux).unwrap(),
            );

            let _ss2 = get_tx_out_shared_secret(
                &tx_private_key,
                &bob_addr.view_public_key(),
            );

            let ss3 = create_shared_secret(bob_addr.view_public_key(), &tx_private_key);
            let ss4 = create_shared_secret(&RistrettoPublic::try_from(&tx_private_key.0 * RISTRETTO_BASEPOINT_POINT).unwrap(), &bob.view_private_key());

            assert_eq!(ss4, ss3);
        }
    }
}

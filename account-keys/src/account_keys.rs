// Copyright (c) 2018-2022 The MobileCoin Foundation

//! MobileCoin account keys.
//!
//! MobileCoin accounts give users fine-grained controls for sharing their
//! address with senders and third-party services. Each account is defined
//! by a pair of private keys (a,b) that are used for identifying owned
//! outputs and spending them, respectively. Instead of sharing the public
//! keys (A,B) directly with senders, users generate and share "subaddresses"
//! (C_i, D_i) that are derived from the private keys (a,b) and an index i.
//! We refer to (C_0, D_0)* as the "default subaddress" for account (a,b).

#![allow(non_snake_case)]

use core::{
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
};
use alloc::string::String;
use curve25519_dalek::{ristretto::RistrettoPoint, scalar::Scalar, constants::RISTRETTO_BASEPOINT_POINT};
use mc_core::{
    keys::{
        RootSpendPrivate, RootSpendPublic, RootViewPrivate, SubaddressSpendPublic,
        SubaddressViewPublic,
    },
    slip10::Slip10Key,
    subaddress::Subaddress,
};
use mc_crypto_digestible::{Digestible, MerlinTranscript};
use mc_crypto_keys::{RistrettoPrivate, RistrettoPublic, ReprBytes, GenericArray};
use mc_util_from_random::FromRandom;
#[cfg(feature = "prost")]
use prost::Message;
use rand_core::{CryptoRng, RngCore};
use serde::{Deserialize, Serialize, ser, de};
use zeroize::Zeroize;

pub use mc_core::{
    account::ShortAddressHash,
    consts::{
        CHANGE_SUBADDRESS_INDEX, DEFAULT_SUBADDRESS_INDEX, GIFT_CODE_SUBADDRESS_INDEX,
        INVALID_SUBADDRESS_INDEX,
    },
};

/// A MobileCoin user's public subaddress.
#[derive(
    Clone, Deserialize, Digestible, Eq, Hash, Ord, PartialEq, PartialOrd, Zeroize, Serialize
)]
#[cfg_attr(feature = "prost", derive(Message))]
pub struct PublicAddress {
    /// The user's public subaddress view key 'C'.
    #[cfg_attr(feature = "prost", prost(message, required, tag = "1"))]
    pub view_public_key: RistrettoPublic,

    /// The user's public subaddress spend key `D`.
    #[cfg_attr(feature = "prost", prost(message, required, tag = "2"))]
    pub spend_public_key: RistrettoPublic,
}

impl fmt::Display for PublicAddress {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MOB")?;
        for byte in self
            .spend_public_key
            .to_bytes()
            .iter()
            .chain(self.view_public_key().to_bytes().iter())
        {
            write!(f, "{byte:02X}")?;
        }
        Ok(())
    }
}

impl PublicAddress {
    /// Create a new public address from CryptoNote key pair (with no account
    /// service)
    ///
    /// # Arguments
    /// `spend_public_key` - The user's public subaddress spend key `D`,
    /// `view_public_key` - The user's public subaddress view key  `C`,
    #[inline]
    pub fn new(spend_public_key: &RistrettoPublic, view_public_key: &RistrettoPublic) -> Self {
        Self {
            view_public_key: *view_public_key,
            spend_public_key: *spend_public_key,
        }
    }

    /// Default
    pub fn default() -> Self {
        Self {
            view_public_key: RistrettoPublic::default(),
            spend_public_key: RistrettoPublic::default(),
        }
    }

    /// Get the public subaddress view key.
    pub fn view_public_key(&self) -> &RistrettoPublic {
        &self.view_public_key
    }

    /// Get the public subaddress spend key.
    pub fn spend_public_key(&self) -> &RistrettoPublic {
        &self.spend_public_key
    }

    /// To bytes
    pub fn to_bytes(&self) -> [u8; 64] {
        let pk1 = self.view_public_key().to_bytes();
        let mut a = pk1.to_vec();
        let pk2 = self.spend_public_key().to_bytes();
        let mut b = pk2.to_vec();
        a.append(&mut b);
        let mut c = [0; 64];
        c.copy_from_slice(&a[..]);
        c
    }

    /// From bytes
    pub fn from_bytes(bytes: [u8; 64]) -> Self {
        let mut a = [0; 32];
        a.copy_from_slice(&bytes[..32]);
        let mut b = [0; 32];
        b.copy_from_slice(&bytes[32..64]);
        let view_public_key = RistrettoPublic::from_bytes(&GenericArray::from(a)).unwrap();
        let spend_public_key = RistrettoPublic::from_bytes(&GenericArray::from(b)).unwrap();

        Self {
            view_public_key,
            spend_public_key,
        }
    }
}

impl mc_account_keys_types::RingCtAddress for PublicAddress {
    fn view_public_key(&self) -> &RistrettoPublic {
        &self.view_public_key
    }

    fn spend_public_key(&self) -> &RistrettoPublic {
        &self.spend_public_key
    }
}

impl mc_core::account::RingCtAddress for PublicAddress {
    fn view_public_key(&self) -> SubaddressViewPublic {
        SubaddressViewPublic::from(self.view_public_key)
    }

    fn spend_public_key(&self) -> SubaddressSpendPublic {
        SubaddressSpendPublic::from(self.spend_public_key)
    }
}

impl From<&PublicAddress> for ShortAddressHash {
    fn from(src: &PublicAddress) -> Self {
        let digest = src.digest32::<MerlinTranscript>(b"mc-address");
        let hash: [u8; 16] = digest[0..16].try_into().expect("arithmetic error");
        Self::from(hash)
    }
}

impl From<&PublicAddress> for mc_core::account::PublicSubaddress {
    fn from(value: &PublicAddress) -> Self {
        Self {
            view_public: value.view_public_key.into(),
            spend_public: value.spend_public_key.into(),
        }
    }
}

impl FromRandom for PublicAddress {
    fn from_random<T: RngCore + CryptoRng>(rng: &mut T) -> Self {
        PublicAddress::new(
            &RistrettoPublic::from_random(rng),
            &RistrettoPublic::from_random(rng),
        )
    }
}

/// Complete AccountKey, containing the pair of secret keys, which can be used
/// for spending, and optionally some fog-related info,
/// can be used for spending. This should only ever be present in client code.
#[derive(Clone, Zeroize, Serialize, Deserialize)]
#[cfg_attr(feature = "prost", derive(Message))]
#[cfg_attr(not(feature = "prost"), derive(Debug))]
#[zeroize(drop)]
pub struct AccountKey {
    /// Private key 'a' used for view-key matching.
    #[cfg_attr(feature = "prost", prost(message, required, tag = "1"))]
    view_private_key: RistrettoPrivate,

    /// Private key `b` used for spending.
    #[cfg_attr(feature = "prost", prost(message, required, tag = "2"))]
    spend_private_key: RistrettoPrivate,
}

// Note: Hash, Ord is implemented in terms of default_subaddress() because
// we don't want comparisons to leak private key details over side-channels.
impl Hash for AccountKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.default_subaddress().hash(state)
    }
}

impl Eq for AccountKey {}

impl PartialEq for AccountKey {
    fn eq(&self, other: &Self) -> bool {
        self.default_subaddress().eq(&other.default_subaddress())
    }
}

impl PartialOrd for AccountKey {
    fn partial_cmp(&self, other: &AccountKey) -> Option<Ordering> {
        self.default_subaddress()
            .partial_cmp(&other.default_subaddress())
    }
}

impl Ord for AccountKey {
    fn cmp(&self, other: &AccountKey) -> Ordering {
        self.default_subaddress().cmp(&other.default_subaddress())
    }
}

/// Create an AccountKey from a SLIP-0010 key
impl From<Slip10Key> for AccountKey {
    fn from(slip10key: Slip10Key) -> Self {
        let spend_private_key = RootSpendPrivate::from(&slip10key);
        let view_private_key = RootViewPrivate::from(&slip10key);

        Self::new(spend_private_key.as_ref(), view_private_key.as_ref())
    }
}

impl AccountKey {
    /// A user's AccountKey, without a fog service.
    ///
    /// # Arguments
    /// * `spend_private_key` - The user's private spend key `b`.
    /// * `view_private_key` - The user's private view key `a`.
    #[inline]
    pub fn new(spend_private_key: &RistrettoPrivate, view_private_key: &RistrettoPrivate) -> Self {
        Self {
            spend_private_key: *spend_private_key,
            view_private_key: *view_private_key,
        }
    }

    /// Default
    pub fn default() -> Self {
        Self {
            spend_private_key: RistrettoPrivate::default(),
            view_private_key: RistrettoPrivate::default(),
        }
    }

    /// Get the view private key.
    pub fn view_private_key(&self) -> &RistrettoPrivate {
        &self.view_private_key
    }

    /// Get the spend private key.
    pub fn spend_private_key(&self) -> &RistrettoPrivate {
        &self.spend_private_key
    }

    /// Create an account key with random secret keys, and no fog service
    /// (intended for tests).
    pub fn random<T: RngCore + CryptoRng>(rng: &mut T) -> Self {
        Self::new(
            &RistrettoPrivate::from_random(rng),
            &RistrettoPrivate::from_random(rng),
        )
    }

    /// Get the account's default subaddress.
    #[inline]
    pub fn to_public_address(&self) -> PublicAddress {
        PublicAddress { view_public_key: RistrettoPublic(self.view_private_key.0 * RISTRETTO_BASEPOINT_POINT), spend_public_key: RistrettoPublic(self.spend_private_key.0 * RISTRETTO_BASEPOINT_POINT) }
    }

    /// Get the account's default subaddress.
    #[inline]
    pub fn default_subaddress(&self) -> PublicAddress {
        self.subaddress(DEFAULT_SUBADDRESS_INDEX)
    }

    /// Get the account's change subaddress.
    #[inline]
    pub fn change_subaddress(&self) -> PublicAddress {
        self.subaddress(CHANGE_SUBADDRESS_INDEX)
    }

    /// Get the account's gift code subaddress.
    #[inline]
    pub fn gift_code_subaddress(&self) -> PublicAddress {
        self.subaddress(GIFT_CODE_SUBADDRESS_INDEX)
    }

    /// Get the account's i^th subaddress.
    pub fn subaddress(&self, index: u64) -> PublicAddress {
        let view_public_key = {
            let subaddress_view_private = self.subaddress_view_private(index);
            RistrettoPublic::from(&subaddress_view_private)
        };

        let spend_public_key = {
            let subaddress_spend_private = self.subaddress_spend_private(index);
            RistrettoPublic::from(&subaddress_spend_private)
        };

        PublicAddress {
            view_public_key,
            spend_public_key,
        }
    }

    /// The private spend key for the default subaddress.
    pub fn default_subaddress_spend_private(&self) -> RistrettoPrivate {
        self.subaddress_spend_private(DEFAULT_SUBADDRESS_INDEX)
    }

    /// The private spend key for the change subaddress.
    pub fn change_subaddress_spend_private(&self) -> RistrettoPrivate {
        self.subaddress_spend_private(CHANGE_SUBADDRESS_INDEX)
    }

    /// The private spend key for the gift code subaddress
    pub fn gift_code_subaddress_spend_private(&self) -> RistrettoPrivate {
        self.subaddress_spend_private(GIFT_CODE_SUBADDRESS_INDEX)
    }

    /// The private spend key for the i^th subaddress.
    pub fn subaddress_spend_private(&self, index: u64) -> RistrettoPrivate {
        let (_view_private, spend_private) = (
            &RootViewPrivate::from(self.view_private_key),
            &RootSpendPrivate::from(self.spend_private_key),
        )
            .subaddress(index);

        spend_private.inner()
    }

    /// The private view key for the default subaddress.
    pub fn default_subaddress_view_private(&self) -> RistrettoPrivate {
        self.subaddress_view_private(DEFAULT_SUBADDRESS_INDEX)
    }

    /// The private view key for the change subaddress.
    pub fn change_subaddress_view_private(&self) -> RistrettoPrivate {
        self.subaddress_view_private(CHANGE_SUBADDRESS_INDEX)
    }

    /// The private view key for the gift code subaddress.
    pub fn gift_code_subaddress_view_private(&self) -> RistrettoPrivate {
        self.subaddress_view_private(GIFT_CODE_SUBADDRESS_INDEX)
    }

    /// The private view key for the i^th subaddress.
    pub fn subaddress_view_private(&self, index: u64) -> RistrettoPrivate {
        let (view_private, _spend_private) = (
            &RootViewPrivate::from(self.view_private_key),
            &RootSpendPrivate::from(self.spend_private_key),
        )
            .subaddress(index);

        view_private.inner()
    }

    /// To bytes
    pub fn to_bytes(&self) -> [u8; 64] {
        let pk1 = self.view_private_key().to_bytes();
        let mut a = pk1.to_vec();
        let pk2 = self.spend_private_key().to_bytes();
        let mut b = pk2.to_vec();
        a.append(&mut b);
        let mut c = [0; 64];
        c.copy_from_slice(&a[..]);
        c
    }

    /// From bytes
    pub fn from_bytes(bytes: [u8; 64]) -> Self {
        let mut a = [0; 32];
        a.copy_from_slice(&bytes[..32]);
        let mut b = [0; 32];
        b.copy_from_slice(&bytes[32..64]);
        let view_private_key = RistrettoPrivate::from_bytes(&GenericArray::from(a)).unwrap();
        let spend_private_key = RistrettoPrivate::from_bytes(&GenericArray::from(b)).unwrap();

        Self {
            view_private_key,
            spend_private_key,
        }
    }
}

#[cfg(test)]
mod account_key_tests {
    use super::*;
    use mc_crypto_keys::RistrettoSignature;
    use mc_test_vectors_account_keys::{
        DefaultSubaddrKeysFromAcctPrivKeys, SubaddrKeysFromAcctPrivKeys,
    };
    use mc_util_test_vector::TestVector;
    use mc_util_test_with_data::test_with_data;
    use rand::prelude::StdRng;
    use rand_core::SeedableRng;

    #[test]
    // Subaddress private keys should agree with subaddress public keys.
    fn test_subadress_private_keys_agree_with_subaddress_public_keys() {
        let mut rng: StdRng = SeedableRng::from_seed([91u8; 32]);
        let view_private = RistrettoPrivate::from_random(&mut rng);
        let spend_private = RistrettoPrivate::from_random(&mut rng);

        let account_key = AccountKey::new(&spend_private, &view_private);

        let index = rng.next_u64();
        let subaddress = account_key.subaddress(index);

        let subaddress_view_private = account_key.subaddress_view_private(index);
        let subaddress_spend_private = account_key.subaddress_spend_private(index);

        let expected_subaddress_view_public = RistrettoPublic::from(&subaddress_view_private);
        let expected_subaddress_spend_public = RistrettoPublic::from(&subaddress_spend_private);

        assert_eq!(expected_subaddress_view_public, subaddress.view_public_key);
        assert_eq!(
            expected_subaddress_spend_public,
            subaddress.spend_public_key
        );
    }

    #[test_with_data(DefaultSubaddrKeysFromAcctPrivKeys::from_jsonl("../test-vectors/vectors"))]
    fn default_subaddr_keys_from_acct_priv_keys(case: DefaultSubaddrKeysFromAcctPrivKeys) {
        let spend_private_key = RistrettoPrivate::try_from(&case.spend_private_key).unwrap();
        let view_private_key = RistrettoPrivate::try_from(&case.view_private_key).unwrap();
        let account_key = AccountKey::new(&spend_private_key, &view_private_key);
        let public_address = account_key.default_subaddress();
        assert_eq!(
            account_key.default_subaddress_view_private().to_bytes(),
            case.subaddress_view_private_key
        );
        assert_eq!(
            account_key.default_subaddress_spend_private().to_bytes(),
            case.subaddress_spend_private_key
        );
        assert_eq!(
            public_address.view_public_key().to_bytes(),
            case.subaddress_view_public_key
        );

        assert_eq!(
            public_address.spend_public_key().to_bytes(),
            case.subaddress_spend_public_key
        );
    }

    #[test_with_data(SubaddrKeysFromAcctPrivKeys::from_jsonl("../test-vectors/vectors"))]
    fn subaddr_keys_from_acct_priv_keys(case: &SubaddrKeysFromAcctPrivKeys) {
        let spend_private_key = RistrettoPrivate::try_from(&case.spend_private_key).unwrap();
        let view_private_key = RistrettoPrivate::try_from(&case.view_private_key).unwrap();
        let account_key = AccountKey::new(&spend_private_key, &view_private_key);
        let public_address = account_key.subaddress(case.subaddress_index);
        assert_eq!(
            account_key
                .subaddress_view_private(case.subaddress_index)
                .to_bytes(),
            case.subaddress_view_private_key
        );
        assert_eq!(
            account_key
                .subaddress_spend_private(case.subaddress_index)
                .to_bytes(),
            case.subaddress_spend_private_key
        );
        assert_eq!(
            public_address.view_public_key().to_bytes(),
            case.subaddress_view_public_key
        );
        assert_eq!(
            public_address.spend_public_key().to_bytes(),
            case.subaddress_spend_public_key
        );
    }
}

#![allow(non_snake_case)]

extern crate rand;

use alloc::vec;
use alloc::vec::Vec;
use curve25519_dalek::ristretto::RistrettoPoint;
use curve25519_dalek::scalar::Scalar;
use curve25519_dalek::traits::Identity;

use crate::{CompressedCommitment, CurveScalar, KeyImage};
use mc_crypto_digestible::Digestible;
use mc_crypto_keys::RistrettoPrivate;
use prost::Message;
use sha2::Sha512;
use std::convert::TryInto;
use curve25519_dalek::constants::RISTRETTO_BASEPOINT_POINT;
use serde::{Deserialize, Serialize};
use crate::Error::InvalidSignature;
use crate::ring_signature::util;
use crate::ring_signature::Error;

/// A curve scalar vec
#[derive(Clone, Serialize, Deserialize, Digestible, Message, PartialEq, Eq, PartialOrd, Ord)]
#[digestible(transparent)]
pub struct CurveScalarVec {
	/// The scalars values
	#[prost(message, repeated, tag = "1")]
	pub scalars: Vec<CurveScalar>,
}

impl From<Vec<CurveScalar>> for CurveScalarVec {
	#[inline]
	fn from(scalars: Vec<CurveScalar>) -> Self {
		Self { scalars }
	}
}

#[derive(Clone, Digestible, PartialEq, Eq, Serialize, Deserialize, Message, PartialOrd, Ord)]
pub struct TriptychEllipticCurveState {
	#[prost(message, required, tag = "1")]
	pub J: CompressedCommitment,
	#[prost(message, required, tag = "2")]
	pub A: CompressedCommitment,
	#[prost(message, required, tag = "3")]
	pub B: CompressedCommitment,
	#[prost(message, required, tag = "4")]
	pub C: CompressedCommitment,
	#[prost(message, required, tag = "5")]
	pub D: CompressedCommitment,
	#[prost(message, repeated, tag = "6")]
	pub X: Vec<CompressedCommitment>,
	#[prost(message, repeated, tag = "7")]
	pub Y: Vec<CompressedCommitment>,
}

impl TriptychEllipticCurveState {
	pub fn default() -> Self {
		Self {
			J: CompressedCommitment::default(),
			A: CompressedCommitment::default(),
			B: CompressedCommitment::default(),
			C: CompressedCommitment::default(),
			D: CompressedCommitment::default(),
			X: vec![],
			Y: vec![],
		}
	}
}

#[derive(Clone, Digestible, Serialize, Deserialize, Message, PartialEq, Eq, PartialOrd, Ord)]
pub struct TriptychScalarState {
	#[prost(message, repeated, tag = "1")]
	pub f: Vec<CurveScalarVec>,
	#[prost(message, required, tag = "2")]
	pub zA: CurveScalar,
	#[prost(message, required, tag = "3")]
	pub zC: CurveScalar,
	#[prost(message, required, tag = "4")]
	pub z: CurveScalar,
}

impl TriptychScalarState {
	pub fn default() -> Self {
		Self {
			zA: CurveScalar::default(),
			zC: CurveScalar::default(),
			z: CurveScalar::default(),
			f: vec![],
		}
	}
}

#[derive(Clone, Digestible, Serialize, Deserialize, Message, PartialEq, Eq, PartialOrd, Ord)]
pub struct TriptychSignature {
	#[prost(message, required, tag = "1")]
	pub a: TriptychEllipticCurveState,
	#[prost(message, required, tag = "2")]
	pub z: TriptychScalarState,
	/// Key image "spent" by this signature.
	#[prost(message, required, tag = "3")]
	pub key_image: KeyImage,
}

impl TriptychSignature {
	pub fn default() -> Self {
		Self {
			a: TriptychEllipticCurveState::default(),
			z: TriptychScalarState::default(),
			key_image: KeyImage::default(),
		}
	}

	pub fn key_image(&self) -> KeyImage {
		self.key_image
	}
}

// This is the core Sigma Protocol being implemented, not the signature protocol
pub fn base_prove(M: &[RistrettoPoint], l: &usize, r: &Scalar, m: &usize, message: &str) -> TriptychSignature {
	let n: usize = 2; // base of decomposition, Tryptich supports arbitary base, we prefer binary here

	let U = util::hash_to_point("U");

	//let G = util::hash_to_point("G");
	let G = RISTRETTO_BASEPOINT_POINT;
	// In Ristretto Curve, all POINTS are generators. G choice is arbitary here
	let mut rng = rand::thread_rng();

	let mut transcript: Vec<u8> = Vec::with_capacity(40000);

	let J = r.invert() * U;
	let rA = Scalar::random(&mut rng);
	let rB = Scalar::random(&mut rng);
	let rC = Scalar::random(&mut rng);
	let rD = Scalar::random(&mut rng);

	let mut a = (0..*m)
		.map(|_| {
			(0..n)
				.map(|_| Scalar::random(&mut rng))
				.collect::<Vec<Scalar>>()
		})
		.collect::<Vec<Vec<Scalar>>>();

	for entry in &mut a {
		entry[0] = (1..n).fold(Scalar::zero(), |acc, x| acc - entry[x]);
	}

	let A = util::pedersen_commitment(&a, &rA);

	for entry in M {
		transcript.extend_from_slice(entry.compress().as_bytes());
	}

	transcript.extend_from_slice(message.as_bytes());
	transcript.extend_from_slice(J.compress().as_bytes());
	transcript.extend_from_slice(A.compress().as_bytes());

	let s = util::pad(&l, &m);

	let b = (0..*m)
		.map(|j| {
			(0..n)
				.map(|i| util::delta(&s[j], &i))
				.collect::<Vec<Scalar>>()
		})
		.collect::<Vec<Vec<Scalar>>>();

	let B = util::pedersen_commitment(&b, &rB);

	let c = (0..*m)
		.map(|j| {
			(0..n)
				.map(|i| a[j][i] * (Scalar::one() - b[j][i] - b[j][i]))
				.collect::<Vec<Scalar>>()
		})
		.collect::<Vec<Vec<Scalar>>>();

	let C = util::pedersen_commitment(&c, &rC);

	let d = (0..*m)
		.map(|j| (0..n).map(|i| -a[j][i] * a[j][i]).collect::<Vec<Scalar>>())
		.collect::<Vec<Vec<Scalar>>>();

	let D = util::pedersen_commitment(&d, &rD);

	transcript.extend_from_slice(B.compress().as_bytes());
	transcript.extend_from_slice(C.compress().as_bytes());
	transcript.extend_from_slice(D.compress().as_bytes());

	let m_u32: u32 = (*m).try_into().unwrap();
	let N = usize::pow(n, m_u32); // we have n = 2, N = 2**m = len(M)

	let mut p = (0..N).map(|_| vec![]).collect::<Vec<Vec<Scalar>>>();

	for k in 0..N {
		let binary_k = util::pad(&k, &m);
		p[k] = vec![a[0][binary_k[0]], util::delta(&s[0], &binary_k[0])];

		for j in 1..*m {
			p[k] = util::convolve(
				&p[k],
				&vec![a[j][binary_k[j]], util::delta(&s[j], &binary_k[j])],
			);
		}
	}

	let rho = (0..*m)
		.map(|_| Scalar::random(&mut rng))
		.collect::<Vec<Scalar>>();

	let Y = (0..*m).map(|i| rho[i] * J).collect::<Vec<RistrettoPoint>>();

	let X = (0..*m)
		.map(|j| (0..N).fold(rho[j] * G, |acc, k| acc + p[k][j] * M[k]))
		.collect::<Vec<RistrettoPoint>>();

	let key_image = KeyImage::from(&RistrettoPrivate(*r));

	for i in 0..*m {
		transcript.extend_from_slice(Y[i].compress().as_bytes());
		transcript.extend_from_slice(X[i].compress().as_bytes());
		transcript.extend_from_slice(key_image.point.as_bytes());
	}

	let ellipticstate: TriptychEllipticCurveState = TriptychEllipticCurveState {
		J: (&J.compress()).into(),
		A: (&A.compress()).into(),
		B: (&B.compress()).into(),
		C: (&C.compress()).into(),
		D: (&D.compress()).into(),
		X: X.iter().map(|input| (&input.compress()).into()).collect(),
		Y: Y.iter().map(|input| (&input.compress()).into()).collect(),
	};

	let challenge = Scalar::hash_from_bytes::<Sha512>(&transcript);

	let f = (0..*m)
		.map(|j| {
			(1..n)
				.map(|i| (util::delta(&s[j], &i) * challenge + a[j][i]).into())
				.collect::<Vec<CurveScalar>>()
				.into()
		})
		.collect::<Vec<CurveScalarVec>>();

	let zA = rA + challenge * rB;
	let zC = challenge * rC + rD;

	let z = r * util::power(&challenge, &m)
		- (0..*m).fold(Scalar::zero(), |acc, j| {
			acc + rho[j] * util::power(&challenge, &j)
		});

	let scalarstate: TriptychScalarState = TriptychScalarState {
		f,
		zA: zA.into(),
		zC: zC.into(),
		z: z.into(),
	};

	return TriptychSignature {
		a: ellipticstate,
		z: scalarstate,
		key_image,
	};
}

// Verification of the base sigma protocol
pub fn base_verify(
	M: &[RistrettoPoint],
	sgn: &TriptychSignature,
	m: &usize,
	message: &str,
) -> Result<(), Error> {
	// The key image must decompress.
	// This ensures that the key image encodes a valid Ristretto point.
	sgn
		.key_image
		.point
		.decompress()
		.ok_or(Error::InvalidKeyImage)?;

	// assert m is a power of 2
	let mut transcript: Vec<u8> = Vec::with_capacity(1000);
	let ellipticState = &sgn.a;
	let scalarState = &sgn.z;
	//let G = util::hash_to_point("G");
	let G = RISTRETTO_BASEPOINT_POINT;
	let U = util::hash_to_point("U");

	let n = 2;
	let m_u32: u32 = (*m).try_into().unwrap();
	let N = usize::pow(n, m_u32); // we have n = 2, N = 2**m = len(M)

	for entry in M {
		transcript.extend_from_slice(entry.compress().as_bytes());
	}
	transcript.extend_from_slice(message.as_bytes());
	transcript.extend_from_slice(ellipticState.J.point.as_bytes());
	transcript.extend_from_slice(ellipticState.A.point.as_bytes());
	transcript.extend_from_slice(ellipticState.B.point.as_bytes());
	transcript.extend_from_slice(ellipticState.C.point.as_bytes());
	transcript.extend_from_slice(ellipticState.D.point.as_bytes());

	for i in 0..*m {
		transcript.extend_from_slice(ellipticState.Y[i].point.as_bytes());
		transcript.extend_from_slice(ellipticState.X[i].point.as_bytes());
		transcript.extend_from_slice(sgn.key_image.point.as_bytes());
	}

	let challenge = Scalar::hash_from_bytes::<Sha512>(&transcript);

	let mut f: Vec<Vec<Scalar>> = vec![vec![Scalar::zero(); n]; *m];

	for i in 0..*m {
		f[i][0] = challenge;
		for j in 1..n {
			f[i][j] = scalarState.f[i].scalars[j - 1].scalar;
			f[i][0] = f[i][0] - f[i][j];
		}
	}

	let comFirst = util::pedersen_commitment(&f, &scalarState.zA.scalar);

	let fMult = (0..*m)
		.map(|j| {
			(0..n)
				.map(|i| f[j][i] * (challenge - f[j][i]))
				.collect::<Vec<Scalar>>()
		})
		.collect::<Vec<Vec<Scalar>>>();

	let comSecond = util::pedersen_commitment(&fMult, &scalarState.zC.scalar);

	let firstLHS = ellipticState.A.point.decompress().unwrap()
		+ ellipticState.B.point.decompress().unwrap() * challenge;
	let secondLHS = ellipticState.D.point.decompress().unwrap()
		+ ellipticState.C.point.decompress().unwrap() * challenge;

	let thirdLHS = (0..*m).fold(scalarState.z.scalar * G, |acc, j| {
		acc + ellipticState.X[j].point.decompress().unwrap() * util::power(&challenge, &j)
	});

	let fourthLHS = (0..*m).fold(
		scalarState.z.scalar * ellipticState.J.point.decompress().unwrap(),
		|acc, j| acc + ellipticState.Y[j].point.decompress().unwrap() * util::power(&challenge, &j),
	);

	let mut thirdRHS = RistrettoPoint::identity();

	let mut fourthRHSScalar = Scalar::zero();
	for k in 0..N {
		let binary_k = util::pad(&k, &m);

		let mut product_term = Scalar::one();

		for j in 0..*m {
			product_term = f[j][binary_k[j]] * product_term;
		}

		thirdRHS = thirdRHS + M[k] * product_term;

		fourthRHSScalar = fourthRHSScalar + product_term;
	}
	let fourthRHS = U * fourthRHSScalar;

	if firstLHS == comFirst
		&& secondLHS == comSecond
		&& thirdLHS == thirdRHS
		&& fourthLHS == fourthRHS
	{
		return Ok(());
	} else {
		return Err(InvalidSignature);
	}
}

pub fn KeyGen() -> (Scalar, RistrettoPoint) {
	//let mut rng = rand::thread_rng();
	//let r = Scalar::random(&mut rng);
	let r = Scalar::one();

	return (r, r * RISTRETTO_BASEPOINT_POINT);
}

pub fn Sign(x: &Scalar, M: &str, R: &[RistrettoPoint]) -> TriptychSignature {
	let G = RISTRETTO_BASEPOINT_POINT;

	let mut l: usize = 0;
	for (i, element) in R.iter().enumerate() {
		if *element == x * G {
			l = i;
		}
	}

	let size = R.len();
	let mut base = 1;
	let mut m = 0;
	while base < size {
		base = base * 2;
		m = m + 1;
	}

	return base_prove(R, &l, x, &m, M);
}

pub fn Verify(sgn: &TriptychSignature, M: &str, R: &[RistrettoPoint]) -> Result<(), Error> {
	let size = R.len();
	let mut base = 1;
	let mut m = 0;

	while base < size {
		base = base * 2;
		m = m + 1;
	}

	return base_verify(R, sgn, &m, M);
}

pub fn Link(sgn_a: &TriptychSignature, sgn_b: &TriptychSignature) -> bool {
	return sgn_a.a.J == sgn_b.a.J;
}

#[cfg(test)]
mod triptych_test {
	use std::time::{Instant, Duration};

use alloc::vec;
	use alloc::vec::Vec;
	use curve25519_dalek::constants::RISTRETTO_BASEPOINT_POINT;
	use curve25519_dalek::ristretto::RistrettoPoint;
	use curve25519_dalek::scalar::Scalar;
	use curve25519_dalek::traits::Identity;
	use crate::ring_signature::{triptych};

	#[test]
	pub fn test_base_signature() {
		let G = RISTRETTO_BASEPOINT_POINT;
		let m: usize = 4;
		let l: usize = 12;
		let len_M = 16;

		let mut M: Vec<RistrettoPoint> = vec![RistrettoPoint::identity(); len_M];

		let mut rng = rand::thread_rng();
		let mut r: Scalar = Scalar::one();
		for i in 0..len_M {
			let sk = Scalar::random(&mut rng);
			M[i] = sk * G;

			if i == l {
				r = sk;
			}
		}

		let sgn: triptych::TriptychSignature = triptych::base_prove(&M, &l, &r, &m, "demo");

		let result = triptych::base_verify(&M, &sgn, &m, "demo");

		assert!(result.is_ok());
	}

	#[test]
	pub fn test_signature() {
		let size = 64;
		let mut R: Vec<RistrettoPoint> = vec![RistrettoPoint::identity(); size];
		let mut x: Scalar = Scalar::one();
		let index = 14;

		for i in 0..size {
			let (sk, pk) = triptych::KeyGen();
			R[i] = pk;

			if i == index {
				x = sk;
			}
		}
		let M = "This is a triptych signature test, lets see if it works or not";

		let sgn = triptych::Sign(&x, &M, &R);

		let start = Instant::now();

		let result = triptych::Verify(&sgn, &M, &R);
		
		let end = Instant::now();
		let duration = end.duration_since(start);
    	let elapsed_ms = Duration::as_millis(&duration);
		println!("Time elapsed: {} ms", elapsed_ms);

		assert!(result.is_ok());
	}
}

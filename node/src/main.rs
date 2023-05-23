

// Copyright(C) Facebook, Inc. and its affiliates.
use anyhow::{Context, Result};
use clap::{crate_name, crate_version, App, AppSettings, ArgMatches, SubCommand};
use config::Export as _;
use config::Import as _;
use config::PK;
use config::SK;
use config::{Committee, KeyPair, Parameters, WorkerId};
use consensus::Consensus;
use curve25519_dalek::scalar::Scalar;
use env_logger::Env;
use mc_account_keys::AccountKey;
use mc_account_keys::PublicAddress;

use mc_crypto_keys::RistrettoPrivate;
use mc_crypto_keys::RistrettoPublic;

use primary::{Certificate, Primary};
use rand_core::OsRng;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver};
use worker::Worker;

/// The default channel capacity.
pub const CHANNEL_CAPACITY: usize = 100_000;

#[tokio::main]
async fn main() -> Result<()> {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about("A research implementation of Narwhal and Tusk.")
        .args_from_usage("-v... 'Sets the level of verbosity'")
        .subcommand(
            SubCommand::with_name("generate_keys")
                .about("Print a fresh key pair to file")
                .args_from_usage("--filename=<FILE> 'The file where to print the new key pair'"),
        )
        .subcommand(
            SubCommand::with_name("run")
                .about("Run a node")
                .args_from_usage("--keys=<FILE> 'The file containing the node keys'")
                .args_from_usage("--committee=<FILE> 'The file containing committee information'")
                .args_from_usage("--parameters=[FILE] 'The file containing the node parameters'")
                .args_from_usage("--store=<PATH> 'The path where to create the data store'")
                //.args_from_usage("--nodes=<INT> 'The number of nodes'")
                .subcommand(SubCommand::with_name("primary").about("Run a single primary"))
                .subcommand(
                    SubCommand::with_name("worker")
                        .about("Run a single worker")
                        .args_from_usage("--id=<INT> 'The worker id'"),
                )
                .setting(AppSettings::SubcommandRequiredElseHelp),
        )
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .get_matches();

    let log_level = match matches.occurrences_of("v") {
        0 => "error",
        1 => "warn",
        2 => "info",
        3 => "debug",
        _ => "trace",
    };
    let mut logger = env_logger::Builder::from_env(Env::default().default_filter_or(log_level));
    #[cfg(feature = "benchmark")]
    logger.format_timestamp_millis();
    logger.init();

    let sk1 = RistrettoPrivate(Scalar::random(&mut OsRng));
    let sk2 = RistrettoPrivate(Scalar::random(&mut OsRng));
    let pk1 = RistrettoPublic::from(&sk1);
    let pk2 = RistrettoPublic::from(&sk2);
    let a: [u8; 32] = *sk1.as_ref();
    let mut b = a.to_vec();
    let c: [u8; 32] = *sk2.as_ref();
    let mut d = c.to_vec();
    b.append(&mut d); 
    let e: [u8; 32] = pk1.as_ref().compress().to_bytes();
    let mut f = e.to_vec();
    let g: [u8; 32] = pk2.as_ref().compress().to_bytes();
    let mut h = g.to_vec();
    f.append(&mut h); 
    let mut i = [0; 64];
    i.copy_from_slice(&f[..]);
    let mut j = [0; 64];
    j.copy_from_slice(&b[..]);
    let keypair = KeyPair {
        name: PK(i),
        secret: SK(j)
    };

    match matches.subcommand() {
        ("generate_keys", Some(sub_matches)) => keypair
            .export(sub_matches.value_of("filename").unwrap())
            .context("Failed to generate key pair")?,
        ("run", Some(sub_matches)) => run(sub_matches).await?,
        _ => unreachable!(),
    }
    Ok(())
}

// Runs either a worker or a primary.
async fn run(matches: &ArgMatches<'_>) -> Result<()> {
    //sleep(std::time::Duration::from_millis(8000));

    //let nodes: u64 = matches.value_of("nodes").unwrap().parse().unwrap();
    let nodes = 0;
    let key_file = matches.value_of("keys").unwrap();
    let committee_file = matches.value_of("committee").unwrap();
    let parameters_file = matches.value_of("parameters");
    let store_path = matches.value_of("store").unwrap();

    // Read the committee and node's keypair from file.
    let keypair = KeyPair::import(key_file).context("Failed to load the node's keypair").unwrap();
    let committee =
        Committee::import(committee_file).context("Failed to load the committee information")?;

    // Load default parameters if none are specified.
    let parameters = match parameters_file {
        Some(filename) => {
            Parameters::import(filename).context("Failed to load the node's parameters")?
        }
        None => Parameters::default(),
    };

    // Make the data store.
    let store = Store::new(store_path).context("Failed to create a store")?;

    // Channels the sequence of certificates.
    let (tx_output, rx_output) = channel(CHANNEL_CAPACITY);

    // Check whether to run a primary, a worker, or an entire authority.
    match matches.subcommand() {
        // Spawn the primary and consensus core.
        ("primary", _) => {
            let (tx_new_certificates, rx_new_certificates) = channel(CHANNEL_CAPACITY);
            let (tx_feedback, rx_feedback) = channel(CHANNEL_CAPACITY);
            Primary::spawn(
                PublicAddress::from_bytes(keypair.name.0),
                AccountKey::from_bytes(keypair.secret.0),
                committee.clone(),
                parameters.clone(),
                store,
                /* tx_consensus */ tx_new_certificates,
                /* rx_consensus */ rx_feedback,
            );
            Consensus::spawn(
                committee,
                parameters.gc_depth,
                /* rx_primary */ rx_new_certificates,
                /* tx_primary */ tx_feedback,
                tx_output,
            );
        }

        // Spawn a single worker.
        ("worker", Some(sub_matches)) => {
            let id = sub_matches
                .value_of("id")
                .unwrap()
                .parse::<WorkerId>()
                .context("The worker id must be a positive integer")?;
            Worker::spawn(PublicAddress::from_bytes(keypair.name.0), id, committee, parameters, store, nodes);
        }
        _ => unreachable!(),
    }

    // Analyze the consensus' output.
    analyze(rx_output).await;

    // If this expression is reached, the program ends and all other tasks terminate.
    unreachable!();
}

/// Receives an ordered list of certificates and apply any application-specific logic.
async fn analyze(mut rx_output: Receiver<Certificate>) {
    while let Some(_certificate) = rx_output.recv().await {
        // NOTE: Here goes the application logic.
    }
}

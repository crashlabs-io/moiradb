use moiradb::{execute_block, kvstore::init, Block, Transaction};
use moiradb_command_examples::replay_beacon::{BeaconCommand, BeaconKey, BeaconState};

use rand::Rng;
use rocksdb::{Options, DB};
use std::sync::Arc;
use tokio;

use std::time;

#[tokio::main]
async fn main() {
    println!("Running moiradb beacon example...");

    // Create the database
    let path = "/tmp/full_beacon_test.rocksdb";
    let _ = DB::destroy(&Options::default(), path);
    let store = DB::open_default(path).expect("database barfed on open");
    let (kv_adapter, handle) = init::<BeaconKey, BeaconState>(store);
    println!("* database created");

    // Populate with initial accounts
    let mut rng = rand::thread_rng();
    const NUM_OF_BEACONS: usize = 10000;

    let mut block: Block<BeaconKey, BeaconCommand> = Vec::with_capacity(NUM_OF_BEACONS);
    block.push(Arc::new(Transaction {
        seq: 0,
        write_set: vec![BeaconKey::CurrentEpoch, BeaconKey::Epoch { epoch: 0 }]
            .into_iter()
            .collect(),
        command: BeaconCommand::SetEpoch { epoch: 0 },
    }));

    for seq in 1usize..NUM_OF_BEACONS {
        let my_beacon = rng.gen::<[u8; 32]>();
        block.push(Arc::new(Transaction {
            seq: seq as u64,
            write_set: vec![
                BeaconKey::Epoch { epoch: 0 },
                BeaconKey::Beacon {
                    key: my_beacon,
                    epoch: 0,
                },
            ]
            .into_iter()
            .collect(),
            command: BeaconCommand::SetBeacon {
                key: my_beacon,
                epoch: 0,
            },
        }));
    }
    println!("* database populated with {} beacons", NUM_OF_BEACONS);

    let now = time::Instant::now();
    let cores = num_cpus::get(); // <- Change number of cores.
    println!("* running beacons using BeaconCommand on {} cores", cores);
    execute_block(block, kv_adapter, cores).await;
    handle.await.unwrap();
    let spent = now.elapsed();
    println!("* block execution finished in {:?}", spent.clone());
    println!("");
    println!(
        "Time per transaction {}us",
        spent.as_micros() / (NUM_OF_BEACONS as u128)
    );
}

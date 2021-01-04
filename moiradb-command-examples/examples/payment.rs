extern crate moiradb;

use moiradb::{execute_block, kvstore::init, Block, Transaction};
use moiradb_command_examples::payment::{Account, AccountKey, PaymentCommand};
use rand::seq::SliceRandom;
use rand::Rng;
use rocksdb::{Options, DB};
use std::sync::Arc;
use tokio;

use std::time;

#[tokio::main]
async fn main() {
    println!("Running moiradb payment example...");

    // Create the database
    let path = "/tmp/full_payment_test.rocksdb";
    let _ = DB::destroy(&Options::default(), path);
    let store = DB::open_default(path).expect("database barfed on open");
    let (mut kv_adapter, handle) = init::<AccountKey, Account>(store);
    println!("* database created");

    // Populate with initial accounts
    let mut rng = rand::thread_rng();
    const NUM_OF_ACCOUNTS: usize = 10000;
    let mut account_keys: Vec<AccountKey> = Vec::with_capacity(NUM_OF_ACCOUNTS);
    for _ in 0..NUM_OF_ACCOUNTS {
        let key = rng.gen::<AccountKey>();

        let account = Account { balance: 1000 };

        kv_adapter.write(vec![(key, Arc::new(Some(account)))]);
        account_keys.push(key);
    }
    println!("* saved 10,000 accounts with random identifiers");

    // Make a block of transactions
    let tx_number = 50_000u128; // <- CHANGE NO OF TX TO TEST SCALING
    let mut block: Block<AccountKey, PaymentCommand> = Vec::with_capacity(tx_number as usize);
    for seq in 0u128..tx_number {
        let payment = PaymentCommand {
            sender: account_keys.choose(&mut rng).unwrap().clone(),
            receiver: account_keys.choose(&mut rng).unwrap().clone(),
            amount: 1,
        };

        let t = Transaction {
            seq: seq as u64,
            write_set: vec![payment.sender.clone(), payment.receiver.clone()]
                .into_iter()
                .collect(),
            command: payment,
        };

        block.push(Arc::new(t));
    }
    println!("* constructed a block with 50,000 transactions using saved accounts");

    let now = time::Instant::now();
    let cores = num_cpus::get(); // <- Change number of cores.
    println!(
        "* running money transfers using PaymentCommand using {} cores",
        cores
    );
    execute_block(block, kv_adapter, cores).await;
    handle.await.unwrap();
    let spent = now.elapsed();
    println!("* block execution finished in {:?}", spent.clone());
    println!("");
    println!("Time per transaction {}us", spent.as_micros() / tx_number);
}

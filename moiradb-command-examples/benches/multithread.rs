use criterion::{criterion_group, criterion_main, Criterion};
use moiradb::{
    execute_block,
    kvstore::{init, KVAdapter},
    Block, Transaction,
};
use moiradb_command_examples::payment::{Account, AccountKey, PaymentCommand};
use rand::{prelude::SliceRandom, Rng};
use rocksdb::{Options, DB};
use std::{sync::Arc, time::Instant};
use tokio::runtime::Runtime;

// Make a block of transactions
fn make_block(size: u128, account_keys: &Vec<AccountKey>) -> Block<AccountKey, PaymentCommand> {
    let mut rng = rand::thread_rng();
    let mut block: Block<AccountKey, PaymentCommand> = Vec::with_capacity(size as usize);
    for seq in 0u128..size {
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
    block
}

async fn make_accounts(
    number_of_accounts: usize,
    kv_adapter: &mut KVAdapter<AccountKey, Account>,
) -> Vec<AccountKey> {
    let mut rng = rand::thread_rng();
    let mut account_keys: Vec<AccountKey> = Vec::with_capacity(number_of_accounts);
    for _ in 0..number_of_accounts {
        let key = rng.gen::<AccountKey>();
        let account = Account { balance: 1000 };
        kv_adapter.write(vec![(key, Arc::new(Some(account)))]);
        account_keys.push(key);
    }
    account_keys
}

fn setup_data_store(test_name: &str) -> KVAdapter<AccountKey, Account> {
    let path = format!("/tmp/bench_{}.rocksdb", test_name);
    let _ = DB::destroy(&Options::default(), path.clone());
    let store = DB::open_default(path).expect("database barfed on open");
    let (kv_adapter, _) = init::<AccountKey, Account>(store);
    kv_adapter
}

fn multithreaded_payment_all_cpus_ten_accounts(c: &mut Criterion) {
    let num_accounts = 10;
    let cores = num_cpus::get(); // use all cores
    let test_name = "multithreaded-all-cpus-10-accounts";
    bench_it(c, test_name, num_accounts, cores);
}

fn multithreaded_payment_one_cpu_ten_accounts(c: &mut Criterion) {
    let num_accounts = 10;
    let cores = 1;
    let test_name = "multithreaded-one-cpu-10-accounts";
    bench_it(c, test_name, num_accounts, cores);
}

fn multithreaded_payment_one_cpu_ten_thousand_accounts(c: &mut Criterion) {
    let num_accounts = 10000;
    let cores = 1;
    let test_name = "multithreaded-one-cpu-10K-accounts";
    bench_it(c, test_name, num_accounts, cores);
}

fn multithreaded_payment_all_cpus_ten_thousand_accounts(c: &mut Criterion) {
    let num_accounts = 10000;
    let cores = num_cpus::get();
    let test_name = "multithreaded-all-cpus-10K-accounts";
    bench_it(c, test_name, num_accounts, cores);
}

fn bench_it(c: &mut Criterion, test_name: &str, num_accounts: usize, cores: usize) {
    let setup_runtime = Runtime::new().unwrap();

    let mut kv_adapter = setup_runtime.block_on(async { setup_data_store(test_name) });

    let account_keys =
        setup_runtime.block_on(async { make_accounts(num_accounts, &mut kv_adapter).await });

    c.bench_function(test_name, |b| {
        b.iter_custom(|iters| {
            let block_size = iters;
            let block = make_block(block_size as u128, &account_keys);
            let kv_adapter = kv_adapter.clone();

            // Create a new runtime for each iteration,
            // slows down benchmark operation but does not
            // affect measurements.
            let runtime = Runtime::new().unwrap();

            let duration = runtime.block_on(async {
                let start = Instant::now();
                execute_block(block.clone(), kv_adapter.clone(), cores).await;
                start.elapsed()
            });

            duration
        });
    });
}

criterion_group!(
    benches,
    multithreaded_payment_all_cpus_ten_accounts,
    multithreaded_payment_one_cpu_ten_accounts,
    multithreaded_payment_one_cpu_ten_thousand_accounts,
    multithreaded_payment_all_cpus_ten_thousand_accounts,
);
criterion_main!(benches);

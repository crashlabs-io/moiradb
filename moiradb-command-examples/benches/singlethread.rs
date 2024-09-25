use criterion::{criterion_group, criterion_main, Criterion};
use moiradb_command_examples::{
    benchmark_singlethread::{execute, NotMoiraTask},
    payment::{Account, AccountKey, PaymentCommand},
};
use rocksdb::{Options, DB};

/// This benchmarks the speed of completely sequential operations.
fn bench_single_threaded_payment(c: &mut Criterion) {
    let path = "/tmp/bench_insert_singlethread.rocksdb";
    let _ = DB::destroy(&Options::default(), path);
    let store = DB::open_default(path).expect("database barfed on open");
    let mut db = NotMoiraTask { seq: 0, store };

    for account in 0u8..255 {
        let key: AccountKey = [account; 32];
        let value = Account {
            balance: 10000000000000000, // big enough that we can subtract lots of 10 without running out of money
        };
        db.write(key, Some(value));
    }

    let payment = PaymentCommand {
        sender: [0; 32],
        receiver: [1; 32],
        amount: 10,
    };

    c.bench_function("execute-single-threaded", |b| {
        b.iter(|| execute(&payment, &mut db))
    });
}

criterion_group!(benches, bench_single_threaded_payment);
criterion_main!(benches);

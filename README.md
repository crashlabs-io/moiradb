# Moiradb

A deterministic database built on top of RocksDb.

Databases that are concerned with strong consistency attempt to achieve *linearizability* - where incoming database transactions can be considered to have executed one-at-a-time, in order. This guarantees that any database state effects arising from transaction `A` were properly applied before any state effects arising from transaction `B`. The ways in which this is achieved can vary. Some databases use sophisticated multi-threaded solutions; others might pessimistically bottleneck the system to ensure consistent transaction execution.

We live in a world of multi-core machines. Is it possible to be optimistically achieve high throughput, but still maintain full consistency? In MoiraDb, some simple rules are applied, in order to do transaction processing across all available CPU cores.

The rules are as follows:

Incoming transactions must already be pre-sorted into a block with a total global ordering applied.

Reads can be handled by any core.

Any insert transaction can be executed on any core, because by definition it does not affect any other transaction (it's new data).

An update transaction which does not contain a reference to any other transaction can be executed on any core (because its state side-effects are self-contained).

Update transactions which contain references to other transactions can be scheduled for execution on any core. In many cases, it can be more efficient overall to execute such transactions more than once (on different cores) as long as state side-effects are applied only once. Paradoxically, lots of modern high-performance systems programming has a mindset of scarcity. But we have abundant computing resources, we often just don't use them to best effect.

## Running examples

From the top level of the repo, you can run:

```
cargo run --release --example beacon
# or
cargo run --release --example payment
```

to get an idea of performance on your machine. As an example, on a three year old Mac M1, I get:

```
Running moiradb payment example...
* database created
* saved 10,000 accounts with random identifiers
* constructed a block with 50,000 transactions using saved accounts
* running money transfers using PaymentCommand on 10 cores
* block execution finished in 230.817583ms

Time per transaction 4us
```

Pretty sweet.

## What's with the name?

In Greek mythology, *Moira* (or the *[Moirai](https://en.wikipedia.org/wiki/Moirai)*) are the entities that control our fates - perfect for a deterministic database.

## Using it in your software

Depend on the `moriadb` crate in `lib`. Use it as a logical layer to achieve higher throughput when you need an in-process key-value store (like RocksDB or similar) and can guarantee blocks which [totally order](https://en.wikipedia.org/wiki/Total_order) transactions before they arrive.

## Should I use this in my software?

No, it is totally experimental. Use it at your own risk. But it's cool!

We all make our own choices.

## Use of RocksDb

RocksDb has been chosen because it's speedy for both reads and writes. We have not yet tried using Rust-native KV stores (e.g. [redb](https://www.redb.org/)), but it might be fun to try it and see what happens to speed. Send a PR!

## Dependencies

On Debian / Ubuntu the following packages are needed:

```
sudo apt install build-essential pkg-config clang gnuplot
```

`gnuplot` is only necessary for graph generation for benchmarks, it's optional.

## Benchmarks

You can run benchmarks using `cargo bench`. We're using Criterion for benchmarking.

## Running perf

In order to do perf stats collection, run

```
cargo build --release
perf record --call-graph=dwarf ./target/release/moiradb
```

The release binary has debug symbols built into it (for now) for easy profiling.

You may need to run the following commands to enable stats collection by non-root users:

```
sudo sh -c 'echo 1 >/proc/sys/kernel/perf_event_paranoid'
sudo sysctl -w kernel.perf_event_paranoid=1
```

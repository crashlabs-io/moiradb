# Moiradb

A deterministic database. 

## Dependencies

On Debian / Ubuntu the following packages are needed:

```
sudo apt install gnuplot build-essential pkg-config clang
```

`gnuplot` is only necessary for graph generation for benchmarks, it's optional. 

## Benchmarks

You can run benchmarks using `cargo bench`. We're using Criterion for this, so benchmarks work in Rust stable. 

Benchmarks generate HTML report pages, output is in `target/criterion/<reportname>/report`.

## Running perf

In order to do perf stats collection, run 

```
cargo build --release
perf record --call-graph=dwarf ./target/release/moiradb
```

You may need to run the following commands to enable stats collection by non-root users:

```
sudo sh -c 'echo 1 >/proc/sys/kernel/perf_event_paranoid'
sudo sysctl -w kernel.perf_event_paranoid=1
```

## What's with the name? 

In Greek mythology, *Moira* (or the *Moirai*) are the entities that control our fates - perfect for a deterministic database.

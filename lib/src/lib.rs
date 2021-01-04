pub use crate::moiradb::{MoiraDb, WriteType};
pub use crate::types::{
    Block, Command, ExecState, MergeCommand, MoiraCommand, Transaction, TransactionResult,
};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::StreamExt;
use kvstore::KVAdapter;
use multistore::MultiVersionedStore;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::VecDeque;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::mpsc;

pub mod kvstore;
pub mod moiradb;
pub mod multistore;
pub mod types;

/* List of generic types we will use (exhaustive):

    - K - Key of the DB (copy, clone, serialize, deserialize)
    - V - Value of the DB (clone, serialize, deserialize)
    - C - Command to the transaction system (serialize, deserialize)

*/

/// Loop through all transactions in a block and execute each of them asynchronously.
pub async fn execute_block<K, V, C>(
    transactions: Block<K, C>,
    kv_adapter: KVAdapter<K, V>,
    cores: usize,
) where
    K: 'static + Send + Sync + Serialize + Eq + Hash + Clone + Debug,
    V: 'static + Send + Sync + Serialize + DeserializeOwned + Debug,
    C: 'static + Send + Sync + Debug + Command<K, V>,
{
    const CONCURRENT_FUTURES: usize = 200;

    let mut multi_versioned_store = MultiVersionedStore::<K, V, C>::new(kv_adapter);
    multi_versioned_store.prepare(&transactions);

    let (command_sender, mut command_receiver) = mpsc::unbounded_channel::<MoiraCommand<K, V, C>>();

    // Run all transactions in separate tasks
    let mut inner_blocks = Vec::<VecDeque<Arc<Transaction<K, C>>>>::with_capacity(cores);
    for _ in 0..cores {
        inner_blocks.push(VecDeque::with_capacity(1 + transactions.len() / cores));
    }

    for i in 0..transactions.len() {
        inner_blocks[i % cores].push_back(transactions[i].clone());
    }

    while let Some(mut inner_block) = inner_blocks.pop() {
        let inner_command_sender = command_sender.clone();

        // We're making on tokio task per inner block
        tokio::spawn(async move {
            // We will make one future per transaction and put them in this bag
            let mut transaction_bag = FuturesUnordered::new();

            // Make and put all transactions as futures in a bag
            let mut idx = 0;
            while let Some(trans) = inner_block.pop_front() {
                let moiradb = MoiraDb::new(trans.clone(), inner_command_sender.clone());
                let fut = moiradb.run_command(); // no .await here! We set up the future but don't run it.
                transaction_bag.push(fut);
                idx += 1;
                if idx == CONCURRENT_FUTURES {
                    break;
                }
            }

            // Now execute them one by one in any order.
            while let Some(_) = transaction_bag.next().await {
                // Add one more
                if let Some(trans) = inner_block.pop_front() {
                    let mdb = MoiraDb::new(trans.clone(), inner_command_sender.clone());
                    let fut = mdb.run_command(); // no wait here!
                    transaction_bag.push(fut);
                }
            }

            // We signal to the server task that no more work is to be done.
            drop(inner_command_sender);
        });
    } // At this point, we're running all the transactions in their own tasks.
      // The next bit will then serve the reads for transactions and receive the results.

    // Drop so that when the tasks also drop it, the server exits.
    drop(command_sender);

    // Run the loop to serve reads and seal outcomes of transactions
    let mut reschedule = VecDeque::new();
    while let Some(cmd) = command_receiver.recv().await {
        match cmd {
            MoiraCommand::Read(key, seq, response) => {
                // Just do the read in the multi-versioned store
                multi_versioned_store.read(&key, seq, response).await;
            }
            MoiraCommand::Outcome(db) => {
                let final_outcome = db.outcome;

                match final_outcome {
                    TransactionResult::Commit => {
                        // println!("Commit {}", db.seq);
                        for key in &db.write_set {
                            if db.kv_store.contains_key(&key) {
                                match &db.kv_store[&key] {
                                    WriteType::Write(val) => {
                                        multi_versioned_store
                                            .write(
                                                &key,
                                                Some(val.clone()),
                                                db.seq,
                                                ExecState::Commit,
                                            )
                                            .await;
                                    }
                                    WriteType::Merge(val) => {
                                        // println!("Merge to key {:?}", key);
                                        multi_versioned_store
                                            .write(
                                                &key,
                                                Some(val.clone()),
                                                db.seq,
                                                ExecState::Merge,
                                            )
                                            .await;
                                    }
                                };
                            } else {
                                multi_versioned_store
                                    .write(key, None, db.seq, ExecState::NoWrite)
                                    .await;
                            }
                        }
                    }
                    TransactionResult::Abort(_) => {
                        // println!("Abort {}", db.seq);
                        for key in &db.write_set {
                            multi_versioned_store
                                .write(key, None, db.seq, ExecState::Abort)
                                .await;
                        }
                    }
                    TransactionResult::Reschedule => {
                        // println!("Reschedule {}", db.seq);
                        for key in &db.write_set {
                            multi_versioned_store
                                .write(key, None, db.seq, ExecState::Reschedule)
                                .await;
                        }
                        reschedule.push_back(db.transaction.clone());
                    }
                }
            }
        }
    }

    // Persist all committed changes.
    multi_versioned_store.commit_all_changes().await;

    // TODO: deal with rescheduled transactions
}

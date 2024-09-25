use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    sync::Arc,
};

use crate::types::{Command, DBValue, MergeCommand, MoiraCommand, Transaction, TransactionResult};
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;
use tokio::sync::{mpsc::UnboundedSender, oneshot};

#[derive(Debug, Clone)]
pub enum WriteType<V> {
    Write(DBValue<V>),
    Merge(DBValue<V>),
}

#[derive(Debug)]
/// There is 1 moira::Task per transaction.
pub struct Task<K, V, C: ?Sized>
where
    C: Debug,
    K: Debug,
    V: Debug,
{
    // Keep private, external code will have access to this object,
    // so we should help devs keep things correct by only exposing
    // the safe interface.
    pub seq: u64,

    /// A channel to the multi version store allowing reads and final write
    read_commands: UnboundedSender<MoiraCommand<K, V, C>>,

    /// A local cache of key/values, read/write
    pub kv_store: HashMap<K, WriteType<V>>,

    /// The keys we expect the transaction to write to
    pub write_set: HashSet<K>,

    /// The outcome of executing the command
    pub outcome: TransactionResult,

    /// A reference to the transaction to execute
    pub transaction: Arc<Transaction<K, C>>,
}

impl<K, V, C> Task<K, V, C>
where
    K: 'static + Send + Serialize + Eq + Hash + Clone + Debug,
    V: 'static + Send + Sync + Serialize + DeserializeOwned + Debug,
    C: Command<K, V> + Debug,
{
    /// Construct a new instance of MoiraTask.
    pub fn new(
        transaction: Arc<Transaction<K, C>>,
        read_commands: UnboundedSender<MoiraCommand<K, V, C>>,
    ) -> Task<K, V, C> {
        Task {
            seq: transaction.seq,
            write_set: transaction.write_set.iter().cloned().collect(),
            read_commands,
            kv_store: HashMap::with_capacity(transaction.write_set.len()),
            outcome: TransactionResult::Reschedule,
            transaction,
        }
    }

    /// Read data from the local cache or, in a case where the data is not cached
    /// locally, from the MultiVersionedStore.
    pub async fn read(&mut self, key: K) -> DBValue<V> {
        // Ensure we return written values on write-read.
        if let Some(type_of_val) = self.kv_store.get(&key) {
            match type_of_val {
                WriteType::Write(value) => {
                    return value.clone();
                }
                WriteType::Merge(_) => {
                    panic!("Cannot read merged values.");
                }
            }
        }

        // First read touches the database.
        let (tx, rx) = oneshot::channel();
        match self
            .read_commands
            .send(MoiraCommand::Read(key, self.seq, tx))
        {
            Err(_) => println!("Error sending command response"),
            Ok(_) => (),
        };

        let (_vec, val) = rx.await.unwrap();
        return val;
    }

    /// Inserts the key and the value into the local key-value cache
    pub async fn write(&mut self, key: K, value: DBValue<V>) {
        // We cache writes here until we know the outcome of the execution.
        self.kv_store.insert(key, WriteType::Write(value));
    }

    /// Inserts a None value for a given key, effectively deleting any stored data.
    pub async fn delete(&mut self, key: K) {
        // We cache writes here until we know the outcome of the execution.
        // TODO: do not re-allocate for each None, use a single one and clone the Arc.
        self.kv_store.insert(key, WriteType::Write(Arc::new(None)));
    }

    /// Sends back results to MoiraTask to commit, or abort or reschedule db writes
    /// to the backing store. Consumes this instance of MoiraTask.
    ///
    /// When a command executes, it returns Abort, Commit, or Reschedule.
    ///
    /// Abort stops execution.
    ///
    /// Commit, and this transaction has only written to the originally
    /// expected values in the write set, we can commit.
    ///
    /// But if it has written to more keys than what was originally expected, we
    /// need to reschedule it for a later commit version. Later, we will re-run the
    /// deferred future(s) and include the proper set of keys, so that the future
    /// result will be consistent.
    pub async fn set_outcome(mut self, state: TransactionResult) {
        self.outcome = match state {
            TransactionResult::Abort(a) => TransactionResult::Abort(a),
            TransactionResult::Reschedule => TransactionResult::Reschedule,
            TransactionResult::Commit => {
                // Check we did not use any write we did not expect.
                let mut outcome = TransactionResult::Commit;
                for (k, _) in &self.kv_store {
                    if !self.write_set.contains(k) {
                        // An unexpected key was written to, reschedule.
                        outcome = TransactionResult::Reschedule;
                        break;
                    }
                }
                outcome
            }
        };

        let return_sender = self.read_commands.clone();

        // consume self by sending it back to the MultiVersionedStore
        return_sender
            .send(MoiraCommand::Outcome(self))
            .expect("channel to MultiVersionStore failed");
    }

    /// Run the command, then consume this MoiraTask in set_outcome()
    pub async fn run_command(mut self) -> u64 {
        // Run the command with MoiraTask
        let transaction = self.transaction.clone();
        let result = transaction.command.execute(&mut self).await;
        // Send back the result to the Moira server task.
        let seq = self.seq;
        self.set_outcome(result).await;
        seq
    }
}

impl<K, V, C> Task<K, V, C>
where
    K: 'static + Send + Serialize + Eq + Hash + Clone + Debug,
    V: 'static + Send + Sync + Serialize + DeserializeOwned + Debug,
    C: Debug + MergeCommand<K, V>,
{
    pub async fn merge(&mut self, key: K, value: DBValue<V>) {
        // We cache writes here until we know the outcome of the execution.
        self.kv_store.insert(key, WriteType::Merge(value));
    }

    pub async fn collect(&mut self, key: K) -> DBValue<V> {
        // First read touches the database.
        let (tx, rx) = oneshot::channel();
        match self
            .read_commands
            .send(MoiraCommand::Read(key.clone(), self.seq, tx))
        {
            Err(_) => println!("Error sending command response"),
            Ok(_) => (),
        };

        let (mut _vec, val) = rx.await.unwrap();

        // Ensure we return written values on write-read.
        if let Some(type_of_val) = self.kv_store.get(&key) {
            match type_of_val {
                WriteType::Write(value) => {
                    return value.clone();
                }
                WriteType::Merge(val) => {
                    _vec.push(val.clone());
                }
            }
        }

        let new_val = C::merge_operator(&key, val, _vec);
        return new_val;
    }
}

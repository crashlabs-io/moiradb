use async_trait::async_trait;
use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;

use tokio::sync::oneshot;

use crate::moiradb::MoiraDb;
pub type DBValue<V> = Arc<Option<V>>;

pub type Block<K, C> = Vec<Arc<Transaction<K, C>>>;

#[async_trait]
pub trait Command<K, V>
where
    K: Debug,
    V: Debug,
    Self: Debug,
{
    async fn execute(&self, db: &mut MoiraDb<K, V, Self>) -> TransactionResult;

    fn merge_operator(_key: &K, _prev_value: DBValue<V>, _updates: Vec<DBValue<V>>) -> DBValue<V> {
        unimplemented!();
    }
}

pub trait MergeCommand<K, V>: Command<K, V>
where
    K: Debug,
    V: Debug,
    Self: Debug,
{
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum ExecState {
    Abort,
    Commit,
    Merge,
    NoWrite,
    Pending,
    Reschedule,
}

#[derive(Debug)]
pub struct Entry<K, V, C> {
    pub transaction: Arc<Transaction<K, C>>,
    pub state: ExecState,
    pub value: Option<DBValue<V>>,
    pub futures: Vec<oneshot::Sender<(Vec<DBValue<V>>, DBValue<V>)>>,
}

#[derive(Debug)]
pub enum MoiraCommand<K, V, C>
where
    C: ?Sized + Debug,
    K: Debug,
    V: Debug,
{
    Read(K, u64, oneshot::Sender<(Vec<DBValue<V>>, DBValue<V>)>),
    Outcome(MoiraDb<K, V, C>),
}

#[derive(Debug)]
pub struct Transaction<K, C: ?Sized> {
    pub seq: u64,
    pub write_set: HashSet<K>,
    pub command: C,
}

#[derive(Debug, PartialEq)]
pub enum TransactionResult {
    Commit,
    Abort(String),
    Reschedule,
}

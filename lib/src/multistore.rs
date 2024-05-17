use std::{
    cmp::Ordering,
    collections::{HashMap, VecDeque},
    fmt::Debug,
    hash::Hash,
    sync::Arc,
};

use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::oneshot;

use crate::{
    kvstore::KVAdapter,
    types::{Block, Command, DBValue, ExecState, Transaction},
};

#[derive(Debug)]
pub struct Read<V> {
    // The index of the start entry for this read.
    // start_entry: usize,
    // Sequence number of read
    seq: u64,
    // The response channel for this read.
    response: oneshot::Sender<(Vec<DBValue<V>>, DBValue<V>)>,
    // Merged collected values on the way.
    merged_values: Vec<DBValue<V>>,
}

#[derive(Debug)]
pub struct Entry<K, V, C> {
    pub seq: u64, // we copy this here, since comparing seems to be hot (from profiling).
    pub transaction: Arc<Transaction<K, C>>,
    pub state: ExecState,
    pub value: Option<DBValue<V>>,
    pub futures: Vec<Read<V>>,
}

pub struct MultiVersionedStore<K, V, C>
where
    K: 'static + Send + Serialize + Eq + Hash + Clone,
    V: 'static + Send + Sync + Serialize + DeserializeOwned,
{
    kv_adapter: KVAdapter<K, V>,
    pub multi_db: HashMap<K, Vec<Entry<K, V, C>>>,

    // Maintain some stats
    pub read_hit: u64,
    pub read_miss: u64,
    pub futures_stored: u64,
    pub read_len: u64,
    pub read_num: u64,
}

impl<K, V, C> MultiVersionedStore<K, V, C>
where
    K: 'static + Send + Serialize + Eq + Hash + Clone + Debug,
    V: 'static + Send + Sync + Serialize + DeserializeOwned + Debug,
    C: Debug + Command<K, V>,
{
    pub fn new(kv_adapter: KVAdapter<K, V>) -> MultiVersionedStore<K, V, C> {
        MultiVersionedStore {
            kv_adapter,
            multi_db: HashMap::new(),

            read_hit: 0,
            read_miss: 0,
            futures_stored: 0,
            read_len: 0,
            read_num: 0,
        }
    }

    pub fn print_stats(&self) {
        println!(
            "HIT: {} MISS: {} FUTURES: {} LEN: {} NUM: {}",
            self.read_hit, self.read_miss, self.futures_stored, self.read_len, self.read_num
        );
    }

    pub fn prepare(&mut self, transaction_block: &Block<K, C>) {
        let mut temp: HashMap<K, VecDeque<Entry<K, V, C>>> = HashMap::new();
        for transaction in transaction_block {
            for key in &transaction.write_set {
                let new_entry: Entry<K, V, C> = Entry {
                    seq: transaction.seq,
                    transaction: transaction.clone(),
                    state: ExecState::Pending,
                    value: None,
                    futures: Vec::new(),
                };
                let entry_list = temp.entry(key.clone()).or_insert(VecDeque::new());
                entry_list.push_front(new_entry);
            }
        }

        for (k, v) in temp {
            self.multi_db.insert(k, v.into_iter().collect());
        }
    }

    pub async fn write(
        &mut self,
        key: &K,
        optional_value: Option<DBValue<V>>,
        seq: u64,
        commit_type: ExecState,
    ) {
        // let entry = self.bisect_entry_simple(key, seq);

        let entry_list = self.multi_db.get_mut(key).unwrap();
        // The format! is always executed, and this is in the critical path. Simplify
        // to unwrap above.
        // .expect(format!("Attempt to write to key {:?} not expected", key).as_str());
        let pos = entry_list
            .binary_search_by(|entry| match entry.seq.cmp(&seq) {
                Ordering::Equal => Ordering::Equal,
                Ordering::Greater => Ordering::Less,
                Ordering::Less => Ordering::Greater,
            })
            .unwrap();
        let entry = entry_list.get_mut(pos).unwrap();
        let mut futures = Vec::with_capacity(entry.futures.len());

        if entry.seq == seq {
            // We found the entry to write!
            // 1. Update the state
            entry.state = commit_type;
            // 2. Update the value
            entry.value = optional_value;
            // 3. Notify waiting reads
            while let Some(read_fut) = entry.futures.pop() {
                self.futures_stored -= 1;
                futures.push(read_fut);
            }
        } else {
            // No entry -- panic
            panic!("Could not find key {:?} at seq {:?}", key, seq);
        }

        while let Some(read_fut) = futures.pop() {
            self.update_future_at(key, read_fut, pos).await;
        }
    }

    async fn update_future_at(&mut self, key: &K, mut read_fut: Read<V>, current_pos: usize) {
        let entry_list = self.multi_db.get_mut(key).unwrap();

        for entry in entry_list.iter_mut().skip(current_pos) {
            self.read_len += 1;
            if entry.seq < read_fut.seq {
                self.read_hit += 1;
                // This is the value we are looking for!
                match entry.state {
                    // A write for this sequence number did not happen,
                    // so we continue looking for a previous value.
                    ExecState::Abort | ExecState::Reschedule | ExecState::NoWrite => continue,
                    ExecState::Commit => {
                        let _ = read_fut
                            .response
                            .send((read_fut.merged_values, entry.value.clone().unwrap()));
                        return;
                    }
                    ExecState::Pending => {
                        entry.futures.push(read_fut);
                        self.futures_stored += 1;
                        return;
                    }
                    ExecState::Merge => {
                        read_fut.merged_values.push(entry.value.clone().unwrap());
                        continue;
                    }
                }
            }
        }

        // Hit the end of the list
        let v = self.kv_adapter.read(key.clone()).await;
        let _ = read_fut.response.send((read_fut.merged_values, v));

        // self.kv_adapter.read_to_fut(key.clone(), read_fut.response);
        self.read_miss += 1;
    }

    pub async fn read(
        &mut self,
        key: &K,
        seq: u64,
        call_back: oneshot::Sender<(Vec<DBValue<V>>, DBValue<V>)>,
    ) {
        match self.multi_db.get_mut(key) {
            None => {
                // No entry -- read from database
                let v = self.kv_adapter.read(key.clone()).await;
                let _ = call_back.send((Vec::new(), v));

                // self.kv_adapter.read_to_fut(key.clone(), call_back);
                self.read_miss += 1;
            }
            Some(entry_list) => {
                self.read_num += 1;

                let idx = entry_list
                    .binary_search_by(|entry| match entry.seq.cmp(&seq) {
                        Ordering::Equal => Ordering::Greater,
                        Ordering::Greater => Ordering::Less,
                        Ordering::Less => Ordering::Greater,
                    })
                    .err()
                    .unwrap();

                let read_fut = Read {
                    // start_entry: idx,
                    seq,
                    response: call_back,
                    merged_values: Vec::new(),
                };

                self.update_future_at(key, read_fut, idx).await;
            }
        };
    }

    pub async fn commit_all_changes(&mut self) {
        let mut write_set: Vec<(K, DBValue<V>)> = Vec::with_capacity(self.multi_db.len());
        let mut all_keys: Vec<K> = self.multi_db.iter().map(|(k, _)| k).cloned().collect();

        while let Some(key) = all_keys.pop() {
            let (tx, rx) = oneshot::channel();
            self.read(&key, u64::MAX, tx).await;
            let (_vec, value): (Vec<DBValue<V>>, DBValue<V>) = rx.await.unwrap();
            let value = if _vec.len() > 0 {
                C::merge_operator(&key, value, _vec)
            } else {
                value
            };
            write_set.push((key.clone(), value.clone()));
        }
        self.kv_adapter.write(write_set);
    }
}

#[cfg(test)]
mod test_prepare {

    use crate::kvstore;
    use crate::moiradb::MoiraDb;
    use crate::types::TransactionResult;
    use async_trait::async_trait;

    use super::*;

    #[async_trait]
    impl Command<String, String> for String {
        async fn execute(&self, _db: &mut MoiraDb<String, String, Self>) -> TransactionResult {
            TransactionResult::Commit
        }
    }

    #[tokio::test]
    async fn test_prepare_one() {
        let kv_adapter = kvstore::setup_database("prepare_one");
        let mut mv_store = MultiVersionedStore::<String, String, String>::new(kv_adapter);
        let block = make_block_one();
        mv_store.prepare(&block);
        assert_eq!(true, mv_store.multi_db.contains_key("A"));
        assert_eq!(true, mv_store.multi_db.contains_key("B"));
        assert_eq!(false, mv_store.multi_db.contains_key("C"));
        assert_eq!(1, mv_store.multi_db.get("A").unwrap().len());
    }

    #[tokio::test]
    async fn test_prepare_many() {
        let kv_adapter = kvstore::setup_database("prepare_many");

        let mut mv_store = MultiVersionedStore::<String, String, String>::new(kv_adapter);
        let block = make_block_many();
        mv_store.prepare(&block);
        assert_eq!(false, mv_store.multi_db.contains_key("H"));
        assert_eq!(2, mv_store.multi_db.get("A").unwrap().len());
        assert_eq!(1, mv_store.multi_db.get("B").unwrap().len());
        assert_eq!(2, mv_store.multi_db.get("C").unwrap().len());
        assert_eq!(1, mv_store.multi_db.get("D").unwrap().len());
        assert_eq!(1, mv_store.multi_db.get("X").unwrap().len());
        assert_eq!(1, mv_store.multi_db.get("Y").unwrap().len());
    }

    #[cfg(test)]
    mod the_multi_versioned_store {
        use super::*;

        #[test]
        fn test_binary_search() {
            let v = vec![10, 7, 3, 1];
            // 5 -> 3 [idx=2]
            let x = v.binary_search_by(|i| match i.cmp(&5) {
                Ordering::Equal => Ordering::Greater,
                Ordering::Greater => Ordering::Less,
                Ordering::Less => Ordering::Greater,
            });
            assert_eq!(Err(2), x);

            // 7 -> 3 [idx=2]
            let x = v.binary_search_by(|i| match i.cmp(&7) {
                Ordering::Equal => Ordering::Less,
                Ordering::Greater => Ordering::Less,
                Ordering::Less => Ordering::Greater,
            });
            assert_eq!(Err(2), x);

            // 1 -> end ?
            let x = v.binary_search_by(|i| match i.cmp(&1) {
                Ordering::Equal => Ordering::Less,
                Ordering::Greater => Ordering::Less,
                Ordering::Less => Ordering::Greater,
            });
            assert_eq!(Err(4), x);
        }

        #[cfg(test)]
        mod reads {
            use super::*;

            mod when_there_is_no_value_in_the_backing_store {
                use super::*;

                #[tokio::test]
                async fn returns_none() {
                    let kv_adapter = kvstore::setup_database("returns_none");
                    let mut mv_store =
                        MultiVersionedStore::<String, String, String>::new(kv_adapter);
                    let (send, read_response) = oneshot::channel();

                    let block = make_block_one();
                    mv_store.prepare(&block);
                    mv_store.read(&"FOO".to_string(), 0, send).await;
                    let (_vec, val) = read_response.await.unwrap();
                    assert_eq!(None, *val);
                }
            }

            mod when_there_is_a_value_in_the_backing_store {
                use super::*;

                #[tokio::test]
                async fn it_returns_the_value() {
                    let mut kv_adapter = kvstore::setup_database("value_in_backing_store");
                    kv_adapter.write(vec![(
                        "EXPECTED_KEY".to_string(),
                        Arc::new(Some("EXPECTED_VALUE".to_string())),
                    )]);
                    let mut mv_store =
                        MultiVersionedStore::<String, String, String>::new(kv_adapter);

                    let (send, read_response) = oneshot::channel();

                    let block = make_block_one();
                    mv_store.prepare(&block);
                    mv_store.read(&"EXPECTED_KEY".to_string(), 0, send).await;
                    let (_vec, val) = read_response.await.unwrap();
                    assert_eq!(Some("EXPECTED_VALUE".to_string()), *val);
                }
            }
        }

        mod writes {
            use super::*;

            mod on_read_and_write {
                use super::*;

                #[tokio::test]
                async fn it_return_the_written_value() {
                    let kv_adapter = kvstore::setup_database("returns_what_I_write");
                    let mut mv_store =
                        MultiVersionedStore::<String, String, String>::new(kv_adapter);
                    let (send, read_response) = oneshot::channel();

                    let block = make_block_one();
                    mv_store.prepare(&block);

                    mv_store
                        .write(
                            &"A".to_string(),
                            Some(Arc::new(Some("MY_VALUE".to_string()))),
                            1,
                            ExecState::Commit,
                        )
                        .await;
                    mv_store.read(&"A".to_string(), 2, send).await;
                    let (_vec, val) = read_response.await.unwrap();
                    assert_eq!(Some("MY_VALUE".to_string()), *val);
                }

                #[tokio::test]
                async fn it_return_what_was_written_even_on_later_read() {
                    let kv_adapter = kvstore::setup_database("returns_what_I_write_even_later");
                    let mut mv_store =
                        MultiVersionedStore::<String, String, String>::new(kv_adapter);
                    let (send, read_response) = oneshot::channel();

                    let block = make_block_one();
                    mv_store.prepare(&block);

                    mv_store.read(&"A".to_string(), 2, send).await;
                    mv_store
                        .write(
                            &"A".to_string(),
                            Some(Arc::new(Some("MY_VALUE".to_string()))),
                            1,
                            ExecState::Commit,
                        )
                        .await;
                    let (_vec, val) = read_response.await.unwrap();
                    assert_eq!(Some("MY_VALUE".to_string()), *val);
                }
            }
        }

        mod aborts {
            use super::*;

            mod on_abort {
                use super::*;

                #[tokio::test]
                async fn reads_give_me_old_value() {
                    let kv_adapter = kvstore::setup_database("reads_give_me_old_value");
                    let mut mv_store =
                        MultiVersionedStore::<String, String, String>::new(kv_adapter);
                    let (send, read_response) = oneshot::channel();

                    let block = make_block_many();
                    mv_store.prepare(&block);

                    mv_store
                        .write(
                            &"A".to_string(),
                            Some(Arc::new(Some("MY_VALUE".to_string()))),
                            1,
                            ExecState::Commit,
                        )
                        .await;
                    mv_store
                        .write(&"A".to_string(), None, 4, ExecState::Abort)
                        .await;
                    mv_store.read(&"A".to_string(), 5, send).await;

                    let (_vec, val) = read_response.await.unwrap();
                    assert_eq!(Some("MY_VALUE".to_string()), *val);
                }

                // This one tests the futures, basically
                #[tokio::test]
                async fn early_reads_give_me_old_value() {
                    let kv_adapter = kvstore::setup_database("early_reads_give_me_old_value");
                    let mut mv_store =
                        MultiVersionedStore::<String, String, String>::new(kv_adapter);
                    let (send, read_response) = oneshot::channel();

                    let block = make_block_many();
                    mv_store.prepare(&block);

                    mv_store.read(&"A".to_string(), 5, send).await;
                    mv_store
                        .write(
                            &"A".to_string(),
                            Some(Arc::new(Some("MY_VALUE".to_string()))),
                            1,
                            ExecState::Commit,
                        )
                        .await;
                    mv_store
                        .write(&"A".to_string(), None, 4, ExecState::Abort)
                        .await;

                    let (_vec, val) = read_response.await.unwrap();
                    assert_eq!(Some("MY_VALUE".to_string()), *val);
                }
            }
        }
    }

    fn make_block_one() -> Block<String, String> {
        let t1 = Transaction {
            seq: 1,
            write_set: vec!["A".to_string(), "B".to_string()].into_iter().collect(),
            command: "T1".to_string(),
        };
        let block = vec![Arc::new(t1)];
        block
    }

    fn make_block_many() -> Block<String, String> {
        let t1 = Transaction {
            seq: 1,
            write_set: vec!["A".to_string(), "B".to_string()].into_iter().collect(),
            command: "T1".to_string(),
        };
        let t2 = Transaction {
            seq: 2,
            write_set: vec!["C".to_string(), "D".to_string()].into_iter().collect(),
            command: "T2".to_string(),
        };
        let t3 = Transaction {
            seq: 3,
            write_set: vec!["Z".to_string()].into_iter().collect(),
            command: "T3".to_string(),
        };
        let t4 = Transaction {
            seq: 4,
            write_set: vec!["A".to_string(), "C".to_string()].into_iter().collect(),
            command: "T4".to_string(),
        };
        let t5 = Transaction {
            seq: 5,
            write_set: vec!["X".to_string(), "Y".to_string()].into_iter().collect(),
            command: "T5".to_string(),
        };

        let block = vec![
            Arc::new(t1),
            Arc::new(t2),
            Arc::new(t3),
            Arc::new(t4),
            Arc::new(t5),
        ];
        block
    }
}

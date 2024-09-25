use crate::payment::{Account, PaymentCommand};
use moiradb::TransactionResult;
use rocksdb::DB;
use serde::{de::DeserializeOwned, Serialize};
use std::hash::Hash;
use std::{thread, time::Duration};

/// A struct with similar methods to MoiraTask that we can use to measure similar
/// operations in a completely single-threaded way.
///
/// This is strictly for benchmarking purposes so we can answer the question
/// "has this thing sped up?"
///
/// You don't ever need to instantiate this, or attempt to emulate it in your
/// own applications - in fact it's a demonstration of what we *don't* want to
/// do.
pub struct NotMoiraTask {
    pub seq: u64,
    pub store: DB, // mock to make things type check
}

impl NotMoiraTask {
    pub fn read<K, V>(&self, key: &K) -> Option<V>
    where
        K: Eq + Hash + Serialize,
        V: Serialize + DeserializeOwned,
    {
        let encoded = bincode::serialize(&key).unwrap();
        match self.store.get(&encoded) {
            Ok(Some(result)) => {
                let thing: V = bincode::deserialize(&result[..]).unwrap();
                return Some(thing);
            }
            Ok(None) => return None,
            Err(err) => panic!("failed on rocksdb retrieval: {}", err),
        }
    }

    pub fn write<K, V>(&mut self, key: K, value: Option<V>)
    where
        K: Eq + Hash + Serialize,
        V: Serialize,
    {
        let encoded_key = bincode::serialize(&key).unwrap();

        match value {
            None => self.store.delete(&encoded_key).unwrap(),
            Some(v) => {
                let encoded_value = bincode::serialize(&v).unwrap();
                self.store.put(&encoded_key, encoded_value).unwrap();
            }
        };
        return;
    }
}

pub fn execute(command: &PaymentCommand, db: &mut NotMoiraTask) -> TransactionResult {
    let sender_record: Option<Account> = db.read(&command.sender);
    let receiver_record: Option<Account> = db.read(&command.receiver);

    if sender_record.is_none() {
        return TransactionResult::Abort(
            format!("account does not exist: {:?}", command.sender).to_string(),
        );
    };

    if receiver_record.is_none() {
        return TransactionResult::Abort(
            format!("account does not exist: {:?}", command.receiver).to_string(),
        );
    };

    let mut sender_account: Account = sender_record.unwrap().clone();
    let mut receiver_account: Account = receiver_record.unwrap().clone();

    let amount = command.amount;
    if !(sender_account.balance >= amount) {
        return TransactionResult::Abort("not enough money".to_string());
    }

    sender_account.balance -= amount;
    receiver_account.balance += amount;

    db.write(command.sender.clone(), Some(sender_account));
    db.write(command.receiver.clone(), Some(receiver_account));
    thread::sleep(Duration::from_millis(1));

    return TransactionResult::Commit;
}

/// These tests do basic verification of single-threaded operations. No benching.
#[cfg(test)]
mod tests {
    use rocksdb::Options;

    use super::*;

    #[test]
    fn insert_and_retrieve() {
        let path = "/tmp/test_insert.rocksdb";
        let _ = DB::destroy(&Options::default(), path);
        let store = DB::open_default(path).expect("database barfed on open");
        let mut db = NotMoiraTask { seq: 0, store };

        let key = [0; 32];
        let value = Account {
            balance: 10000000000000000, // big enough that we can subtract lots of 10 without running out of money
        };
        db.write(key.clone(), Some(value.clone()));

        let result: Account = db.read(&key).unwrap();
        assert_eq!(value, result);
    }

    #[test]
    fn delete_on_no_value() {
        let path = "/tmp/test_delete.rocksdb";
        let _ = DB::destroy(&Options::default(), path);
        let store = DB::open_default(path).expect("database barfed on open");
        let mut db = NotMoiraTask { seq: 0, store };

        let key = [0; 32];
        let value = Account {
            balance: 10000000000000000, // big enough that we can subtract lots of 10 without running out of money
        };
        db.write(key.clone(), Some(value.clone()));
        let none: Option<Account> = None;
        db.write(key.clone(), none);
        let result: Option<Account> = db.read(&key);
        assert_eq!(None, result);
    }
}

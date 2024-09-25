use async_trait::async_trait;
use moiradb::{Command, Task, TransactionResult};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
// use std::{thread, time::Duration};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Account {
    pub balance: u64,
}

pub type AccountKey = [u8; 32];

#[derive(Debug)]
pub struct PaymentCommand {
    pub sender: AccountKey,
    pub receiver: AccountKey,
    pub amount: u64,
}

#[async_trait]
impl Command<AccountKey, Account> for PaymentCommand {
    async fn execute(
        &self,
        db: &mut Task<AccountKey, Account, PaymentCommand>,
    ) -> TransactionResult {
        if self.sender == self.receiver {
            return TransactionResult::Abort("Same sender and receiver.".to_string());
        }

        let sender_record: Arc<Option<Account>> = db.read(self.sender.clone()).await;
        let receiver_record: Arc<Option<Account>> = db.read(self.receiver.clone()).await;

        if sender_record.is_none() {
            return TransactionResult::Abort(
                format!("account does not exist: {:?}", self.sender).to_string(),
            );
        };

        if receiver_record.is_none() {
            return TransactionResult::Abort(
                format!("account does not exist: {:?}", self.receiver).to_string(),
            );
        };

        let mut sender_account: Account = (*sender_record).clone().unwrap();
        let mut receiver_account: Account = (*receiver_record).clone().unwrap();

        let amount = self.amount;
        if !(sender_account.balance >= amount) {
            return TransactionResult::Abort("not enough money".to_string());
        }

        sender_account.balance -= amount;
        receiver_account.balance += amount;

        db.write(self.sender.clone(), Arc::new(Some(sender_account)))
            .await;
        db.write(self.receiver.clone(), Arc::new(Some(receiver_account)))
            .await;

        // thread::sleep(Duration::from_millis(1));
        return TransactionResult::Commit;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use moiradb::{Block, Transaction};
    use rand::seq::SliceRandom;
    use rand::Rng;
    use rocksdb::{Options, DB};
    use std::time;

    fn make_tx(
        sender: AccountKey,
        receiver: AccountKey,
        amount: u64,
        seq: u64,
    ) -> Transaction<AccountKey, PaymentCommand> {
        let payment = PaymentCommand {
            sender,
            receiver,
            amount,
        };

        Transaction {
            seq,
            write_set: vec![payment.sender.clone(), payment.receiver.clone()]
                .into_iter()
                .collect(),
            command: payment,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn payment_test_correct() {
        let path = "/tmp/payment_test_correct.rocksdb";
        let _ = DB::destroy(&Options::default(), path);
        let store = DB::open_default(path).expect("database barfed on open");
        let (mut kv_adapter, _) = moiradb::kvstore::init::<AccountKey, Account>(store);

        // Random numbers
        let mut rng = rand::thread_rng();

        let alice_key = rng.gen::<AccountKey>();
        let alice_account = Account { balance: 100 };

        let bob_key = rng.gen::<AccountKey>();
        let bob_account = Account { balance: 200 };

        let charlie_key = rng.gen::<AccountKey>();
        let charlie_account = Account { balance: 400 };

        kv_adapter.write(vec![
            (alice_key, Arc::new(Some(alice_account))),
            (bob_key, Arc::new(Some(bob_account))),
            (charlie_key, Arc::new(Some(charlie_account))),
        ]);

        let mut block: Block<AccountKey, PaymentCommand> = Vec::with_capacity(10);
        let t1 = make_tx(alice_key, bob_key, 10, 10);
        let t2 = make_tx(alice_key, bob_key, 10000, 20);
        let t3 = make_tx(bob_key, charlie_key, 210, 30);
        let t4 = make_tx(charlie_key, alice_key, 5, 40);

        block.push(Arc::new(t1));
        block.push(Arc::new(t2));
        block.push(Arc::new(t3));
        block.push(Arc::new(t4));

        let cores = 4; // <- Change number of cores.
        moiradb::execute_block(block, kv_adapter.clone(), cores).await;

        assert_eq!(
            95u64,
            kv_adapter
                .read(alice_key)
                .await
                .as_ref()
                .as_ref()
                .unwrap()
                .balance
        );
        assert_eq!(
            0u64,
            kv_adapter
                .read(bob_key)
                .await
                .as_ref()
                .as_ref()
                .unwrap()
                .balance
        );
        assert_eq!(
            605u64,
            kv_adapter
                .read(charlie_key)
                .await
                .as_ref()
                .as_ref()
                .unwrap()
                .balance
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn full_payment_test() {
        // Create the database
        let path = "/tmp/full_payment_test.rocksdb";
        let _ = DB::destroy(&Options::default(), path);
        let store = DB::open_default(path).expect("database barfed on open");
        let (mut kv_adapter, _) = moiradb::kvstore::init::<AccountKey, Account>(store);

        // Random numbers
        let mut rng = rand::thread_rng();

        // Populate with initial accounts
        let mut account_keys: Vec<AccountKey> = Vec::with_capacity(2000);
        for _ in 0..100 {
            let key = rng.gen::<AccountKey>();

            let account = Account { balance: 1000 };

            kv_adapter.write(vec![(key, Arc::new(Some(account)))]);
            account_keys.push(key);
        }

        // Make a block of transactions
        let tx_number = 100u128; // <- CHANGE NO OF TX TO TEST SCALING
        let mut block: Block<AccountKey, PaymentCommand> = Vec::with_capacity(tx_number as usize);
        for seq in 0u128..tx_number {
            let payment = PaymentCommand {
                sender: account_keys.choose(&mut rng).unwrap().clone(),
                receiver: account_keys.choose(&mut rng).unwrap().clone(),
                amount: 1,
            };

            if payment.sender == payment.receiver {
                continue;
            }

            let t = Transaction {
                seq: seq as u64,
                write_set: vec![payment.sender.clone(), payment.receiver.clone()]
                    .into_iter()
                    .collect(),
                command: payment,
            };

            block.push(Arc::new(t));
        }

        let now = time::Instant::now();
        let cores = 4; // <- Change number of cores.
        moiradb::execute_block(block, kv_adapter, cores).await;

        let spent = now.elapsed();
        println!("Time per transaction {}us", spent.as_micros() / tx_number);
    }
}

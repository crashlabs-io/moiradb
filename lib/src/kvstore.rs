use super::*;
use rocksdb::{WriteBatch, DB};

use std::fmt::Debug;

use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

type DBValue<V> = Arc<Option<V>>;

#[derive(Debug)]
pub enum KVCommand<K, V>
where
    K: Send,
    V: Send,
{
    Read(K, oneshot::Sender<DBValue<V>>),
    Write(Vec<(K, DBValue<V>)>),
}

#[derive(Clone)]
pub struct KVAdapter<K, V>
where
    K: Send,
    V: Send,
{
    pub sender: mpsc::UnboundedSender<KVCommand<K, V>>,
}

impl<K, V> KVAdapter<K, V>
where
    K: Debug + Send,
    V: Debug + Send,
{
    pub async fn read(&mut self, key: K) -> DBValue<V> {
        let (send, response) = oneshot::channel();
        if let Err(e) = self.sender.send(KVCommand::Read(key, send)) {
            panic!("SendError: {:?}", e);
        }
        response.await.unwrap()
    }

    pub fn read_to_fut(&mut self, key: K, fut: oneshot::Sender<DBValue<V>>) {
        if let Err(e) = self.sender.send(KVCommand::Read(key, fut)) {
            panic!("SendError: {:?}", e);
        }
    }

    pub fn write(&mut self, write_set: Vec<(K, DBValue<V>)>) {
        if let Err(e) = self.sender.send(KVCommand::Write(write_set)) {
            panic!("SendError: {:?}", e);
        }
    }
}

/*
pub struct KVBackend<K,V> {
    pub kvstore : DB,
    pub cache : HashMap<K,V>,
    pub receiver : mpsc::Receiver<KVCommand<K,V>>,
}
*/

// question: is this not basically a new? Can we put it into the impl?
pub fn init<K, V>(db: DB) -> (KVAdapter<K, V>, JoinHandle<()>)
where
    K: 'static + Send + Serialize + Debug,
    V: 'static + Send + Sync + Serialize + DeserializeOwned + Debug,
{
    let (tx, mut rx) = mpsc::unbounded_channel();

    let handle = tokio::spawn(async move {
        while let Some(command) = rx.recv().await {
            match command {
                KVCommand::Read(key, response) => {
                    let key_bytes = bincode::serialize(&key).unwrap();
                    let value = db.get_pinned(key_bytes).unwrap();
                    if let Some(value_bytes) = value {
                        let parsed_value: V = bincode::deserialize(&value_bytes[..]).unwrap();
                        match response.send(Arc::new(Some(parsed_value))) {
                            Err(e) => println!(
                                "Error {:?} sending parsed response value for key: {:?}",
                                e, key
                            ),
                            _ => (),
                        };
                    } else {
                        match response.send(Arc::new(None)) {
                            Err(e) => {
                                println!("Error {:?} sending empty response for key: {:?}", e, key)
                            }
                            _ => (),
                        };
                    }
                }
                KVCommand::Write(mut write_set) => {
                    let mut batch = WriteBatch::default();
                    while let Some((key, value)) = write_set.pop() {
                        let key_bytes = bincode::serialize(&key).unwrap();
                        match &*value {
                            None => batch.delete(key_bytes),
                            Some(value) => {
                                let value_bytes = bincode::serialize(&value).unwrap();
                                batch.put(key_bytes, value_bytes);
                            }
                        }
                    }
                    match db.write(batch) {
                        Ok(_) => {}
                        Err(e) => println!("Error writing to db: {:?}", e),
                    }; // Atomically commits the batch
                }
            }
        }

        match db.flush() {
            Ok(_) => {}
            Err(e) => println!("Error flushing database: {:?}", e),
        };
    });

    let adapter: KVAdapter<K, V> = KVAdapter { sender: tx };
    (adapter, handle)
}

#[cfg(test)]
mod tests {
    use rocksdb::Options;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn kvstore_read_and_write() {
        let path = "/tmp/test_kvstore_read_and_write.rocksdb";
        let _ = DB::destroy(&Options::default(), path);
        let store = DB::open_default(path).expect("database barfed on open");

        let (mut kv_adapter, _) = init::<String, String>(store);

        // Read a key that does not exist returns None
        let v = kv_adapter.read("A".to_string()).await;
        assert_eq!(None, *v);

        // Write a value
        kv_adapter.write(vec![("A".to_string(), Arc::new(Some("VA".to_string())))]);

        // Read the value written
        let v = kv_adapter.read("A".to_string()).await;
        assert_eq!(Some("VA".to_string()), *v);

        // Delete the key A
        kv_adapter.write(vec![("A".to_string(), Arc::new(None))]);

        // Read the deleted key returns None
        let v = kv_adapter.read("A".to_string()).await;
        assert_eq!(None, *v);
    }
}

#[cfg(test)]
pub fn setup_database(test_name: &str) -> KVAdapter<String, String> {
    let path = format!("/tmp/test_{}.rocksdb", test_name);
    let _ = DB::destroy(&rocksdb::Options::default(), path.clone());
    let store = DB::open_default(path).expect("database barfed on open");
    let (kv_adapter, _) = init::<String, String>(store);
    kv_adapter
}

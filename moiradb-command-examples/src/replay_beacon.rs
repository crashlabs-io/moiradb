use async_trait::async_trait;
use moiradb::types::DBValue;
use moiradb::{Command, MergeCommand, MoiraDb, TransactionResult};

use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BeaconKey {
    Beacon { key: [u8; 32], epoch: u64 },
    Epoch { epoch: u64 },
    CurrentEpoch,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum BeaconState {
    Beacon,
    EpochSet { keys: Vec<BeaconKey> },
    CurrentEpoch { epoch: u64 },
    BeaconUpdate(BeaconKey),
}

#[derive(Debug)]
pub enum BeaconCommand {
    SetEpoch { epoch: u64 },
    SetBeacon { key: [u8; 32], epoch: u64 },
}

impl MergeCommand<BeaconKey, BeaconState> for BeaconCommand {}

#[async_trait]
impl Command<BeaconKey, BeaconState> for BeaconCommand {
    fn merge_operator(
        _key: &BeaconKey,
        prev_value: DBValue<BeaconState>,
        updates: Vec<DBValue<BeaconState>>,
    ) -> DBValue<BeaconState> {
        let mut keys = if let BeaconState::EpochSet { keys } = prev_value.as_ref().clone().unwrap()
        {
            keys
        } else {
            panic!("Wrong value type.");
        };

        keys.reserve(updates.len());
        println!("Added {} keys", updates.len());
        keys.extend(updates.iter().map(|item| {
            if let BeaconState::BeaconUpdate(k) = item.as_ref().as_ref().unwrap() {
                k
            } else {
                panic!("wrong type of update!");
            }
        }));

        Arc::new(Some(BeaconState::EpochSet { keys }))
    }

    async fn execute(
        &self,
        db: &mut MoiraDb<BeaconKey, BeaconState, BeaconCommand>,
    ) -> TransactionResult {
        match self {
            /* The logic to register a beacon as used */
            BeaconCommand::SetBeacon {
                key: cmd_key,
                epoch: cmd_epoch,
            } => {
                let current_epoch = db.read(BeaconKey::CurrentEpoch).await;
                match *current_epoch {
                    Some(BeaconState::CurrentEpoch { epoch: current }) => {
                        // Only valid within the epoch
                        if *cmd_epoch != current {
                            return TransactionResult::Abort("Wrong epoch.".to_string());
                        }

                        // Check if the beacon already exists
                        let beacon_key = BeaconKey::Beacon {
                            key: *cmd_key,
                            epoch: *cmd_epoch,
                        };
                        let stored_beacon = db.read(beacon_key.clone()).await;
                        if stored_beacon.is_some() {
                            return TransactionResult::Abort("Beacon already used.".to_string());
                        }

                        // Write the beacon
                        db.write(beacon_key, Arc::new(Some(BeaconState::Beacon)))
                            .await;

                        // Write the index to the beacon
                        // THIS IS THE INEFFICIENT BIT
                        let use_merge = true;

                        if !use_merge {
                            let epoch_index_key = BeaconKey::Epoch { epoch: current };
                            let mut index = (*db.read(epoch_index_key).await).clone().unwrap();

                            if let BeaconState::EpochSet { keys: idx_keys } = &mut index {
                                idx_keys.push(beacon_key);
                            }

                            db.write(epoch_index_key, Arc::new(Some(index))).await;
                        } else {
                            let epoch_index_key = BeaconKey::Epoch { epoch: current };
                            let update = BeaconState::BeaconUpdate(beacon_key);
                            db.merge(epoch_index_key, Arc::new(Some(update))).await;
                        }

                        return TransactionResult::Commit;
                    }
                    _ => {
                        return TransactionResult::Abort("Wrong structure.".to_string());
                    }
                }
            }

            /* The Logic that advances the epoch */
            BeaconCommand::SetEpoch { epoch: my_epoch } => {
                // Read the current epoch
                let current_epoch = db.read(BeaconKey::CurrentEpoch).await;
                match *current_epoch {
                    None => {
                        if *my_epoch != 0 {
                            return TransactionResult::Abort("New epoch must be zero".to_string());
                        }
                        // Set the current epoch to zero
                        db.write(
                            BeaconKey::CurrentEpoch,
                            Arc::new(Some(BeaconState::CurrentEpoch { epoch: 0 })),
                        )
                        .await;
                        // Write an empty index for epoch zero
                        db.write(
                            BeaconKey::Epoch { epoch: 0 },
                            Arc::new(Some(BeaconState::EpochSet { keys: Vec::new() })),
                        )
                        .await;

                        return TransactionResult::Commit;
                    }
                    Some(BeaconState::CurrentEpoch { epoch: current }) => {
                        if *my_epoch != current + 1 {
                            return TransactionResult::Abort("New epoch must follow".to_string());
                        }
                        // Set the current epoch to my_epoch
                        db.write(
                            BeaconKey::CurrentEpoch,
                            Arc::new(Some(BeaconState::CurrentEpoch { epoch: *my_epoch })),
                        )
                        .await;

                        // Write an empty index for epoch my_epoch
                        db.write(
                            BeaconKey::Epoch { epoch: *my_epoch },
                            Arc::new(Some(BeaconState::EpochSet { keys: Vec::new() })),
                        )
                        .await;

                        // Clean up keys from past epoch
                        let v = db.read(BeaconKey::Epoch { epoch: current }).await;
                        if let Some(BeaconState::EpochSet { keys: prev_keys }) = Option::as_ref(&v)
                        {
                            for key in prev_keys {
                                db.delete(*key).await;
                            }
                        } else {
                            return TransactionResult::Abort("Wrong state type.".to_string());
                        }

                        return TransactionResult::Commit;
                    }
                    Some(_) => {
                        return TransactionResult::Abort("Wrong state type.".to_string());
                    }
                }
            }
        };
    }
}

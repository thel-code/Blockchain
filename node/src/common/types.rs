use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Encode, Decode, Clone)]
pub struct Transaction {
    pub from: String,
    pub to: String,
    pub amount: u64,
}


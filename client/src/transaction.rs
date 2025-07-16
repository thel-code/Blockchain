use bincode::Encode;
use serde;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Encode)]
pub struct Transaction {
    pub from : String,
    pub to : String,
    pub amount : u64,
}
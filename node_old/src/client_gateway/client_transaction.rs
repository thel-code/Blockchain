use bincode::{Decode, Encode};
use serde;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Decode, Debug)]
pub struct ClientTransaction {
    pub from : String,
    pub to : String,
    pub amount : u64,
}
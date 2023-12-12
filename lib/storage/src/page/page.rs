use common::PageId;
use getset::{CopyGetters, Getters, Setters};
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

#[derive(
    Debug,
    Default,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Getters,
    CopyGetters,
    Setters,
    TypedBuilder,
)]
pub struct Page {
    #[getset(get_copy = "pub")]
    id: PageId,
    #[getset(get = "pub")]
    data: Vec<u8>,
    #[getset(get_copy = "pub", set = "pub")]
    is_dirty: bool,
    #[getset(get_copy = "pub", set = "pub")]
    pin_count: u32,
}

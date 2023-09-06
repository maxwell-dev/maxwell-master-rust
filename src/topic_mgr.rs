use std::borrow::Borrow;
use std::sync::Arc;

use anyhow::Result;
use bytes::{Bytes, BytesMut};
use once_cell::sync::Lazy;
use quick_cache::{sync::Cache, Weighter};
use seriesdb::{
  coder::Coder,
  prelude::Db,
  table::{NormalTable, Table, TableEnhanced},
};

use crate::db::DB;
use crate::node_mgr::NodeId;

type Topic = String;
type Store = TableEnhanced<NormalTable, Topic, NodeId, TopicCoder>;

pub struct TopicCoder;

impl Coder<Topic, NodeId> for TopicCoder {
  type EncodedKey = Bytes;
  type EncodedValue = Bytes;

  #[inline(always)]
  fn encode_key<K: Borrow<Topic>>(key: K) -> Self::EncodedKey {
    BytesMut::from(key.borrow().as_bytes()).freeze()
  }

  #[inline(always)]
  fn decode_key(key: &[u8]) -> Topic {
    std::str::from_utf8(key).unwrap().to_string()
  }

  #[inline(always)]
  fn encode_value<V: Borrow<NodeId>>(value: V) -> Self::EncodedValue {
    BytesMut::from(value.borrow().as_bytes()).freeze()
  }

  #[inline(always)]
  fn decode_value(value: &[u8]) -> NodeId {
    std::str::from_utf8(value).unwrap().to_string()
  }
}

#[derive(Clone)]
pub struct TopicWeighter;

impl Weighter<String, String> for TopicWeighter {
  fn weight(&self, key: &String, val: &String) -> u32 {
    (key.len() + val.len()) as u32
  }
}

pub struct TopicMgr {
  pub(crate) cache: Cache<Topic, NodeId, TopicWeighter>,
  pub(crate) store: Arc<Store>,
}

impl TopicMgr {
  fn new(store: Arc<Store>) -> Self {
    let cache = Cache::with_weighter(10000, 10000 as u64 * 64, TopicWeighter);
    TopicMgr { cache, store }
  }

  pub fn assign(&self, topic: Topic, backend_id: NodeId) -> Result<()> {
    let topic_bytes = <TopicCoder as Coder<Topic, NodeId>>::encode_key(&topic);
    let backend_id_bytes = <TopicCoder as Coder<Topic, NodeId>>::encode_value(&backend_id);
    self.cache.insert(topic, backend_id);
    Ok(self.store.raw().put(topic_bytes, backend_id_bytes)?)
  }

  pub fn locate(&self, topic: &Topic) -> Result<Option<NodeId>> {
    let backend_id = self.cache.get(topic);
    if backend_id.is_some() {
      Ok(backend_id)
    } else {
      if let Some(backend_id) = self.store.get(topic)? {
        self.cache.insert(topic.clone(), backend_id.clone());
        Ok(Some(backend_id))
      } else {
        Ok(None)
      }
    }
  }
}

pub static TOPIC_MGR: Lazy<TopicMgr> = Lazy::new(|| {
  TopicMgr::new(Arc::new(DB.open_table("topics").unwrap().enhance::<Topic, NodeId, TopicCoder>()))
});

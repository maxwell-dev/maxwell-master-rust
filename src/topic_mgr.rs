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

use crate::node_mgr::NodeId;
use crate::{db::DB, node_mgr::BACKEND_MGR};

type Topic = String;
type TopicStore = TableEnhanced<NormalTable, Topic, NodeId, TopicCoder>;

struct TopicCoder;

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

type InfoKey = String;
type InfoValue = String;
type InfoStore = TableEnhanced<NormalTable, InfoKey, InfoValue, InfoCoder>;

struct InfoCoder;

impl Coder<InfoKey, InfoValue> for InfoCoder {
  type EncodedKey = Bytes;
  type EncodedValue = Bytes;

  #[inline(always)]
  fn encode_key<K: Borrow<InfoKey>>(key: K) -> Self::EncodedKey {
    BytesMut::from(key.borrow().as_bytes()).freeze()
  }

  #[inline(always)]
  fn decode_key(key: &[u8]) -> InfoKey {
    std::str::from_utf8(key).unwrap().to_string()
  }

  #[inline(always)]
  fn encode_value<V: Borrow<InfoValue>>(value: V) -> Self::EncodedValue {
    BytesMut::from(value.borrow().as_bytes()).freeze()
  }

  #[inline(always)]
  fn decode_value(value: &[u8]) -> InfoValue {
    std::str::from_utf8(value).unwrap().to_string()
  }
}

#[derive(Clone)]
struct TopicWeighter;

impl Weighter<String, String> for TopicWeighter {
  fn weight(&self, key: &String, val: &String) -> u64 {
    (key.len() + val.len()) as u64
  }
}

pub struct TopicMgr {
  cache: Cache<Topic, NodeId, TopicWeighter>,
  topic_store: Arc<TopicStore>,
  info_store: Arc<InfoStore>,
}

impl TopicMgr {
  #[inline]
  fn new(topic_store: Arc<TopicStore>, info_store: Arc<InfoStore>) -> Self {
    let cache = Cache::with_weighter(10000, 10000 as u64 * 64, TopicWeighter);
    let topic_mgr = TopicMgr { cache, topic_store, info_store };
    topic_mgr.check();
    topic_mgr
  }

  #[inline]
  pub fn assign(&self, topic: Topic, backend_id: NodeId) -> Result<()> {
    let topic_bytes = <TopicCoder as Coder<Topic, NodeId>>::encode_key(&topic);
    let backend_id_bytes = <TopicCoder as Coder<Topic, NodeId>>::encode_value(&backend_id);
    self.cache.insert(topic, backend_id);
    Ok(self.topic_store.raw().put(topic_bytes, backend_id_bytes)?)
  }

  #[inline]
  pub fn locate(&self, topic: &Topic) -> Result<Option<NodeId>> {
    let backend_id = self.cache.get(topic);
    if backend_id.is_some() {
      Ok(backend_id)
    } else {
      if let Some(backend_id) = self.topic_store.get(topic)? {
        self.cache.insert(topic.clone(), backend_id.clone());
        Ok(Some(backend_id))
      } else {
        Ok(None)
      }
    }
  }

  // Deletes all topics if the checksum of backends changed
  #[inline]
  fn check(&self) {
    let info_key = "backend_checksum".to_owned();
    let curr_backend_checksum = format!("{}", BACKEND_MGR.checksum());
    if let Some(old_backend_checksum) = self.info_store.get(&info_key).unwrap() {
      if curr_backend_checksum != old_backend_checksum {
        log::info!(
          "The checksum of backends changed from: [{:?}] to: [{:?}]",
          old_backend_checksum,
          curr_backend_checksum
        );
        self.info_store.put(info_key, curr_backend_checksum).unwrap();
        DB.truncate_table("topic_mgr.topics").unwrap();
      }
    } else {
      self.info_store.put(info_key, curr_backend_checksum).unwrap();
    }
  }
}

pub static TOPIC_MGR: Lazy<TopicMgr> = Lazy::new(|| {
  TopicMgr::new(
    Arc::new(DB.open_table("topic_mgr.topics").unwrap().enhance::<Topic, NodeId, TopicCoder>()),
    Arc::new(DB.open_table("topic_mgr.infos").unwrap().enhance::<InfoKey, InfoValue, InfoCoder>()),
  )
});

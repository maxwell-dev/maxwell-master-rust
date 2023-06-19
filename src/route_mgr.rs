use std::sync::Arc;
use std::{borrow::Borrow, collections::HashSet};

use ahash::RandomState as AHasher;
use bytes::{Bytes, BytesMut};
use dashmap::{iter::Iter, DashMap};
use once_cell::sync::Lazy;
use seriesdb::{
  coder::Coder,
  prelude::Db,
  table::{NormalTable, Table, TableEnhanced},
};

use crate::db::DB;
use crate::node_mgr::NodeId;

type Path = String;
type PathSet = HashSet<Path, AHasher>;
type Store = TableEnhanced<NormalTable, NodeId, PathSet, RouteCoder>;

pub struct RouteCoder;

impl Coder<NodeId, PathSet> for RouteCoder {
  type EncodedKey = Bytes;
  type EncodedValue = Bytes;

  #[inline(always)]
  fn encode_key<K: Borrow<NodeId>>(key: K) -> Self::EncodedKey {
    BytesMut::from(key.borrow().as_bytes()).freeze()
  }

  #[inline(always)]
  fn decode_key(key: &[u8]) -> NodeId {
    std::str::from_utf8(key).unwrap().to_string()
  }

  #[inline(always)]
  fn encode_value<V: Borrow<PathSet>>(value: V) -> Self::EncodedValue {
    bincode::serialize(value.borrow()).unwrap().into()
  }

  #[inline(always)]
  fn decode_value(value: &[u8]) -> PathSet {
    bincode::deserialize(value).unwrap()
  }
}

pub struct RouteMgr {
  pub(crate) cache: DashMap<NodeId, PathSet, AHasher>,
  pub(crate) store: Arc<Store>,
}

impl RouteMgr {
  fn new(store: Arc<Store>) -> Self {
    let cache = DashMap::with_capacity_and_hasher(512, AHasher::default());
    let route_mgr = RouteMgr { cache, store };
    route_mgr.recover();
    route_mgr
  }

  pub fn add_reverse_route_group(&self, server_id: NodeId, paths: Vec<Path>) {
    let path_set = paths.into_iter().collect();
    let server_id_bytes = <RouteCoder as Coder<NodeId, PathSet>>::encode_key(&server_id);
    let path_set_bytes = <RouteCoder as Coder<NodeId, PathSet>>::encode_value(&path_set);
    self.cache.insert(server_id, path_set);
    self.store.raw().put(server_id_bytes, path_set_bytes).unwrap_or_else(|err| {
      log::warn!("Failed to add reverse route group into store: {:?}", err);
    });
  }

  pub fn reverse_route_group_iter(&self) -> Iter<NodeId, PathSet, AHasher> {
    self.cache.iter()
  }

  fn recover(&self) {
    let mut cursor = self.store.new_cursor();
    cursor.seek_to_first();
    while cursor.is_valid() {
      let server_id = cursor.key().unwrap();
      let path_set = cursor.value().unwrap();
      self.cache.insert(server_id, path_set);
      cursor.next();
    }
  }
}

pub static ROUTE_MGR: Lazy<RouteMgr> = Lazy::new(|| {
  RouteMgr::new(Arc::new(DB.open_table("routes").unwrap().enhance::<NodeId, PathSet, RouteCoder>()))
});

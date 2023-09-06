use std::sync::{
  atomic::{AtomicU32, Ordering},
  Arc,
};
use std::{borrow::Borrow, collections::HashSet};

use ahash::RandomState as AHasher;
use bytes::{Bytes, BytesMut};
use dashmap::{iter::Iter, mapref::entry::Entry, DashMap};
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
  pub(crate) version: AtomicU32,
}

impl RouteMgr {
  fn new(store: Arc<Store>) -> Self {
    let cache = DashMap::with_capacity_and_hasher(512, AHasher::default());
    let route_mgr = RouteMgr { cache, store, version: AtomicU32::new(0) };
    route_mgr.recover();
    route_mgr
  }

  pub fn add_reverse_route_group(&self, service_id: NodeId, paths: Vec<Path>) {
    let path_set = paths.into_iter().collect();
    let service_id_bytes = <RouteCoder as Coder<NodeId, PathSet>>::encode_key(&service_id);
    let path_set_bytes = <RouteCoder as Coder<NodeId, PathSet>>::encode_value(&path_set);
    match self.cache.entry(service_id) {
      Entry::Occupied(mut entry) => {
        if entry.get() != &path_set {
          entry.insert(path_set);
          self.store.raw().put(service_id_bytes, path_set_bytes).unwrap_or_else(|err| {
            log::warn!("Failed to add reverse route group into store: {:?}", err);
          });
          self.version.fetch_add(1, Ordering::SeqCst);
        }
      }
      Entry::Vacant(entry) => {
        entry.insert(path_set);
        self.store.raw().put(service_id_bytes, path_set_bytes).unwrap_or_else(|err| {
          log::warn!("Failed to add reverse route group into store: {:?}", err);
        });
        self.version.fetch_add(1, Ordering::SeqCst);
      }
    }
  }

  pub fn remove_reverse_route_group(&self, service_id: &NodeId) {
    if self.cache.remove(service_id).is_some() {
      self.store.delete(service_id).unwrap_or_else(|err| {
        log::warn!("Failed to remove reverse route group from store: {:?}", err);
      });
      self.version.fetch_add(1, Ordering::SeqCst);
    }
  }

  pub fn reverse_route_group_iter(&self) -> Iter<NodeId, PathSet, AHasher> {
    self.cache.iter()
  }

  pub fn version(&self) -> u32 {
    self.version.load(Ordering::SeqCst)
  }

  fn recover(&self) {
    let mut cursor = self.store.new_cursor();
    cursor.seek_to_first();
    while cursor.is_valid() {
      let service_id = cursor.key().unwrap();
      let path_set = cursor.value().unwrap();
      self.cache.insert(service_id, path_set);
      cursor.next();
    }
  }
}

pub static ROUTE_MGR: Lazy<RouteMgr> = Lazy::new(|| {
  RouteMgr::new(Arc::new(DB.open_table("routes").unwrap().enhance::<NodeId, PathSet, RouteCoder>()))
});

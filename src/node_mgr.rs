use std::{borrow::Borrow, fmt::Debug, net::IpAddr, sync::Arc};

use ahash::RandomState as AHasher;
use bytes::{Bytes, BytesMut};
use chrono::prelude::*;
use dashmap::{mapref::one::Ref, DashMap};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use seriesdb::{
  coder::Coder,
  prelude::Db,
  table::{NormalTable, Table, TableEnhanced},
};

use crate::db::DB;

pub type NodeId = String;
type Store<N> = TableEnhanced<NormalTable, NodeId, N, NodeCoder>;

pub trait Node: Clone + Debug {
  fn id(&self) -> &NodeId;
  fn active_at(&self) -> u32;
  fn set_active_at(&mut self, active_at: u32);
  fn as_bytes(&self) -> Bytes;
  fn from_bytes(bytes: &[u8]) -> Self;
}

pub struct NodeCoder;

impl<N: Node> Coder<NodeId, N> for NodeCoder {
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
  fn encode_value<V: Borrow<N>>(value: V) -> Self::EncodedValue {
    value.borrow().as_bytes()
  }

  #[inline(always)]
  fn decode_value(value: &[u8]) -> N {
    N::from_bytes(value)
  }
}

pub struct NodeMgr<N: Node> {
  pub(crate) cache: DashMap<NodeId, N, AHasher>,
  pub(crate) store: Arc<Store<N>>,
}

impl<N: Node> NodeMgr<N> {
  #[inline]
  pub(crate) fn new(store: Arc<Store<N>>) -> Self {
    let cache = DashMap::with_capacity_and_hasher(64, AHasher::default());
    let node_mgr = NodeMgr { cache, store };
    node_mgr.recover();
    node_mgr
  }

  pub fn add(&self, node: N) {
    let id_bytes = <NodeCoder as Coder<NodeId, N>>::encode_key(node.id());
    let node_bytes = <NodeCoder as Coder<NodeId, N>>::encode_value(&node);
    self.cache.insert(node.id().clone(), node);
    self
      .store
      .raw()
      .put(id_bytes, node_bytes)
      .unwrap_or_else(|err| log::warn!("Failed to add node: err: {:?}", err));
  }

  // pub fn remove(&self, id: &NodeId) {
  //   self.cache.remove(id);
  //   self.store.delete(id).unwrap_or_else(|err| log::warn!("Failed to remove node: err: {:?}", err));
  // }

  pub fn activate(&self, id: &NodeId) {
    if let Some(mut node) = self.cache.get_mut(id) {
      let active_time = node.active_at();
      let now = Utc::now().timestamp() as u32;
      node.set_active_at(now);
      if now - active_time > 60 {
        self
          .store
          .put(id, &*node)
          .unwrap_or_else(|err| log::warn!("Failed to activate node: err: {:?}", err));
      }
    }
  }

  pub fn get<'a>(&'a self, id: &NodeId) -> Option<Ref<'a, NodeId, N, AHasher>> {
    self.cache.get(id)
  }

  fn recover(&self) {
    let mut cursor = self.store.new_cursor();
    cursor.seek_to_first();
    while cursor.is_valid() {
      let id = cursor.key().unwrap();
      let node = cursor.value().unwrap();
      println!("id {:?}  node: {:?}", id, node);
      self.cache.insert(id, node);
      cursor.next();
    }
  }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Frontend {
  id: String,
  http_port: u32,
  https_port: u32,
  public_ip: IpAddr,
  private_ip: IpAddr,
  active_at: u32,
}

impl Frontend {
  pub fn new(public_ip: IpAddr, private_ip: IpAddr, http_port: u32, https_port: u32) -> Self {
    Frontend {
      id: format!("{:?}:{:?}", private_ip, http_port),
      public_ip,
      private_ip,
      http_port,
      https_port,
      active_at: 0,
    }
  }
}

impl Node for Frontend {
  fn id(&self) -> &NodeId {
    &self.id
  }

  fn active_at(&self) -> u32 {
    self.active_at
  }

  fn set_active_at(&mut self, active_at: u32) {
    self.active_at = active_at;
  }

  fn as_bytes(&self) -> Bytes {
    bincode::serialize(self).unwrap().into()
  }

  fn from_bytes(bytes: &[u8]) -> Self {
    bincode::deserialize(bytes).unwrap()
  }
}

pub type FrontendMgr = NodeMgr<Frontend>;

pub static FRONTEND_MGR: Lazy<FrontendMgr> = Lazy::new(|| {
  FrontendMgr::new(Arc::new(
    DB.open_table("frontends").unwrap().enhance::<NodeId, Frontend, NodeCoder>(),
  ))
});

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Backend {
  id: String,
  http_port: u32,
  private_ip: IpAddr,
  active_at: u32,
}

impl Backend {
  pub fn new(private_ip: IpAddr, http_port: u32) -> Self {
    Backend { id: format!("{:?}:{:?}", private_ip, http_port), private_ip, http_port, active_at: 0 }
  }
}

impl Node for Backend {
  fn id(&self) -> &NodeId {
    &self.id
  }

  fn active_at(&self) -> u32 {
    self.active_at
  }

  fn set_active_at(&mut self, active_at: u32) {
    self.active_at = active_at;
  }

  fn as_bytes(&self) -> Bytes {
    bincode::serialize(self).unwrap().into()
  }

  fn from_bytes(bytes: &[u8]) -> Self {
    bincode::deserialize(bytes).unwrap()
  }
}

pub type BackendMgr = NodeMgr<Backend>;

pub static BACKEND_MGR: Lazy<BackendMgr> = Lazy::new(|| {
  BackendMgr::new(Arc::new(
    DB.open_table("backends").unwrap().enhance::<NodeId, Backend, NodeCoder>(),
  ))
});

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Server {
  id: String,
  http_port: u32,
  private_ip: IpAddr,
  active_at: u32,
}

impl Server {
  pub fn new(private_ip: IpAddr, http_port: u32) -> Self {
    Server { id: format!("{:?}:{:?}", private_ip, http_port), http_port, private_ip, active_at: 0 }
  }
}

impl Node for Server {
  fn id(&self) -> &NodeId {
    &self.id
  }

  fn active_at(&self) -> u32 {
    self.active_at
  }

  fn set_active_at(&mut self, active_at: u32) {
    self.active_at = active_at;
  }

  fn as_bytes(&self) -> Bytes {
    bincode::serialize(self).unwrap().into()
  }

  fn from_bytes(bytes: &[u8]) -> Self {
    bincode::deserialize(bytes).unwrap()
  }
}

pub type ServerMgr = NodeMgr<Server>;

pub static SERVER_MGR: Lazy<ServerMgr> = Lazy::new(|| {
  ServerMgr::new(Arc::new(
    DB.open_table("backends").unwrap().enhance::<NodeId, Server, NodeCoder>(),
  ))
});

#[derive(Debug, Clone, Copy)]
pub enum NodeType {
  Unknown,
  Frontend,
  Backend,
  Server,
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use chrono::prelude::*;
  use serde::{Deserialize, Serialize};
  use seriesdb::prelude::{Db, NormalDb, Options};

  use super::*;

  #[derive(Debug, Clone, Serialize, Deserialize)]
  struct NodeTest {
    id: String,
    active_at: u32,
  }

  impl NodeTest {
    pub fn new(id: String, active_at: u32) -> Self {
      NodeTest { id, active_at }
    }
  }

  impl Node for NodeTest {
    fn id(&self) -> &NodeId {
      &self.id
    }

    fn active_at(&self) -> u32 {
      self.active_at
    }

    fn set_active_at(&mut self, active_at: u32) {
      self.active_at = active_at;
    }

    fn as_bytes(&self) -> Bytes {
      bincode::serialize(self).unwrap().into()
    }

    fn from_bytes(bytes: &[u8]) -> Self {
      bincode::deserialize(bytes).unwrap()
    }
  }

  #[test]
  fn test_basic() {
    let db = Arc::new(NormalDb::open("data/test_basic", &mut Options::new()).unwrap());
    let table = Arc::new(db.open_table("node_test_mgr").unwrap().enhance());
    let node_mgr = NodeMgr::<NodeTest>::new(table);
    let id = "1".to_owned();
    let input_node = NodeTest::new(id.clone(), Utc::now().timestamp() as u32);
    node_mgr.add(input_node);
    let output_node = node_mgr.get(&id);
    assert_eq!(output_node.unwrap().value().id(), &id);
  }
}

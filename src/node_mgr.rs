use std::{borrow::Borrow, fmt::Debug, net::IpAddr, ops::Deref, sync::Arc};

use ahash::RandomState as AHasher;
use atomptr::AtomPtr;
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

pub struct NodeBox<N: Node> {
  pub(crate) node: N,
  pub(crate) prev_id: AtomPtr<Option<NodeId>>,
}

pub struct NodeRef<'a, N: Node> {
  node_box_ref: Ref<'a, NodeId, NodeBox<N>, AHasher>,
}

// impl<'a, N: Node> NodeRef<'a, N> {
//   pub fn key(&self) -> &NodeId {
//     self.node_box_ref.key()
//   }

//   pub fn value(&self) -> &N {
//     &self.node_box_ref.node
//   }
// }

impl<'a, N: Node> Deref for NodeRef<'a, N> {
  type Target = N;

  fn deref(&self) -> &N {
    &self.node_box_ref.node
  }
}

pub struct NodeMgr<N: Node> {
  pub(crate) cache: DashMap<NodeId, NodeBox<N>, AHasher>,
  pub(crate) store: Arc<Store<N>>,
  pub(crate) last_id: AtomPtr<Option<NodeId>>,
  pub(crate) next_access_id: AtomPtr<Option<NodeId>>,
}

impl<N: Node> NodeMgr<N> {
  #[inline]
  pub(crate) fn new(store: Arc<Store<N>>) -> Self {
    let cache = DashMap::with_capacity_and_hasher(64, AHasher::default());
    let node_mgr =
      NodeMgr { cache, store, last_id: AtomPtr::new(None), next_access_id: AtomPtr::new(None) };
    node_mgr.recover();
    node_mgr
  }

  pub fn add(&self, node: N) {
    let id_bytes = <NodeCoder as Coder<NodeId, N>>::encode_key(node.id());
    let node_bytes = <NodeCoder as Coder<NodeId, N>>::encode_value(&node);
    match self.cache.entry(node.id().clone()) {
      dashmap::mapref::entry::Entry::Occupied(mut entry) => {
        entry.get_mut().node = node;
      }
      dashmap::mapref::entry::Entry::Vacant(entry) => {
        let prev_id = self.swap_last_id(node.id().clone());
        entry.insert(NodeBox { node, prev_id });
      }
    }
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
    if let Some(mut node_box) = self.cache.get_mut(id) {
      let active_time = node_box.node.active_at();
      let now = Utc::now().timestamp() as u32;
      node_box.node.set_active_at(now);
      if now - active_time > 60 {
        self
          .store
          .put(id, &node_box.node)
          .unwrap_or_else(|err| log::warn!("Failed to activate node: err: {:?}", err));
      }
    }
  }

  pub fn get<'a>(&'a self, id: &NodeId) -> Option<NodeRef<'a, N>> {
    if let Some(node_box) = self.cache.get(id) {
      Some(NodeRef { node_box_ref: node_box })
    } else {
      None
    }
  }

  pub fn next<'a>(&'a self) -> Option<NodeRef<'a, N>> {
    if let Some(next_access_id) = &**self.next_access_id.get_ref() {
      if let Some(node_box) = self.cache.get(next_access_id) {
        self.next_access_id.swap((**node_box.prev_id.get_ref()).clone());
        Some(NodeRef { node_box_ref: node_box })
      } else {
        self.next_access_id.swap(None);
        None
      }
    } else {
      if let Some(last_id) = &**self.last_id.get_ref() {
        if let Some(node_box) = self.cache.get(last_id) {
          self.next_access_id.swap((**node_box.prev_id.get_ref()).clone());
          Some(NodeRef { node_box_ref: node_box })
        } else {
          None
        }
      } else {
        None
      }
    }
  }

  fn recover(&self) {
    let mut cursor = self.store.new_cursor();
    cursor.seek_to_first();
    while cursor.is_valid() {
      let id = cursor.key().unwrap();
      let node = cursor.value().unwrap();

      let prev_id = self.swap_last_id(id.clone());
      self.cache.insert(id, NodeBox { node, prev_id });

      cursor.next();
    }
  }

  fn swap_last_id(&self, id: NodeId) -> AtomPtr<Option<NodeId>> {
    let last_id: AtomPtr<Option<String>> = if let Some(last_id) = &**self.last_id.get_ref() {
      AtomPtr::new(Some(last_id.clone()))
    } else {
      AtomPtr::new(None)
    };
    self.last_id.swap(Some(id));
    last_id
  }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Frontend {
  pub(crate) id: String,
  pub(crate) http_port: u32,
  pub(crate) https_port: u32,
  pub(crate) public_ip: IpAddr,
  pub(crate) private_ip: IpAddr,
  pub(crate) active_at: u32,
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
  pub(crate) id: String,
  pub(crate) private_ip: IpAddr,
  pub(crate) http_port: u32,
  pub(crate) active_at: u32,
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
  pub(crate) id: String,
  pub(crate) private_ip: IpAddr,
  pub(crate) http_port: u32,
  pub(crate) active_at: u32,
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
  ServerMgr::new(Arc::new(DB.open_table("servers").unwrap().enhance::<NodeId, Server, NodeCoder>()))
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
    db.truncate_table("node_test_mgr").unwrap();
    let table = Arc::new(db.open_table("node_test_mgr").unwrap().enhance());

    let node_mgr = NodeMgr::<NodeTest>::new(table);

    assert!(node_mgr.next().is_none());
    assert!(node_mgr.next().is_none());

    let id = "1".to_owned();
    let input_node = NodeTest::new(id.clone(), Utc::now().timestamp() as u32);
    node_mgr.add(input_node);
    let output_node = node_mgr.get(&id);
    assert_eq!(output_node.unwrap().id(), &id);

    assert_eq!(node_mgr.next().unwrap().id(), &id);
    assert_eq!(node_mgr.next().unwrap().id(), &id);
    assert_eq!(node_mgr.next().unwrap().id(), &id);

    let id2 = "2".to_owned();
    let input_node2 = NodeTest::new(id2.clone(), Utc::now().timestamp() as u32);
    node_mgr.add(input_node2);

    assert_eq!(node_mgr.next().unwrap().id(), &id2);
    assert_eq!(node_mgr.next().unwrap().id(), &id);
    assert_eq!(node_mgr.next().unwrap().id(), &id2);
    assert_eq!(node_mgr.next().unwrap().id(), &id);
  }
}

use std::net::IpAddr;

use ahash::RandomState as AHasher;
use chrono::Utc;
use dashmap::DashMap;
use once_cell::sync::Lazy;

use super::{build_node_id, Node, NodeId, NodeIter, NodeRef};
use crate::config::CONFIG;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Backend {
  pub(crate) id: String,
  pub(crate) private_ip: IpAddr,
  pub(crate) http_port: u32,
  pub(crate) active_at: u32,
}

impl Backend {
  pub fn new(private_ip: IpAddr, http_port: u32) -> Self {
    Backend { id: build_node_id(private_ip, http_port), private_ip, http_port, active_at: 0 }
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
}

pub type BackendRef<'a> = NodeRef<'a, Backend>;
pub type BackendIter<'a> = NodeIter<'a, Backend>;

pub struct BackendMgr {
  pub(crate) backends: DashMap<NodeId, Backend, AHasher>,
  pub(crate) backend_ids: Vec<NodeId>,
}

impl BackendMgr {
  #[inline]
  pub(crate) fn new() -> Self {
    let backends = DashMap::with_capacity_and_hasher(64, AHasher::default());
    let mut backend_mgr = BackendMgr { backends, backend_ids: Vec::with_capacity(64) };
    backend_mgr.initialize();
    backend_mgr
  }

  #[inline]
  pub fn activate(&self, id: &NodeId) {
    if let Some(mut backend) = self.backends.get_mut(id) {
      backend.active_at = Utc::now().timestamp() as u32;
    }
  }

  #[inline]
  pub fn get<'a>(&'a self, id: &NodeId) -> Option<BackendRef<'a>> {
    if let Some(backend) = self.backends.get(id) {
      Some(backend)
    } else {
      None
    }
  }

  #[inline]
  pub fn pick_with<'a, F>(&'a self, with: F) -> Option<BackendRef<'a>>
  where F: Fn(&'a DashMap<NodeId, Backend, AHasher>, &'a Vec<NodeId>) -> Option<&'a NodeId> {
    with(&self.backends, &self.backend_ids).and_then(|backend_id| self.backends.get(backend_id))
  }

  #[allow(dead_code)]
  #[inline]
  pub fn iter<'a>(&'a self) -> BackendIter<'a> {
    self.backends.iter()
  }

  #[inline]
  fn initialize(&mut self) {
    CONFIG.backend_mgr.backends.iter().for_each(|backend_config| {
      let backend = Backend::new(backend_config.private_ip, backend_config.http_port);
      self.backend_ids.push(backend.id.clone());
      self.backends.insert(backend.id.clone(), backend.clone());
    });
    self.backend_ids.sort();
  }
}

pub static BACKEND_MGR: Lazy<BackendMgr> = Lazy::new(|| BackendMgr::new());

use std::net::IpAddr;

use ahash::RandomState as AHasher;
use chrono::Utc;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use rand::{thread_rng, Rng};

use super::{build_node_id, Node, NodeId, NodeIter, NodeRef, NodeRefMulti};
use crate::config::CONFIG;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Frontend {
  pub(crate) id: NodeId,
  pub(crate) domain: String,
  pub(crate) http_port: u32,
  pub(crate) https_port: u32,
  pub(crate) public_ip: IpAddr,
  pub(crate) private_ip: IpAddr,
  pub(crate) active_at: u32,
}

impl Frontend {
  pub fn new(
    domain: String, public_ip: IpAddr, private_ip: IpAddr, http_port: u32, https_port: u32,
  ) -> Self {
    Frontend {
      id: build_node_id(private_ip, http_port),
      domain,
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
}

pub type FrontendRef<'a> = NodeRef<'a, Frontend>;
pub type FrontendRefMulti<'a> = NodeRefMulti<'a, Frontend>;
pub type FrontendIter<'a> = NodeIter<'a, Frontend>;

pub struct FrontendMgr {
  pub(crate) frontends: DashMap<NodeId, Frontend, AHasher>,
}

impl FrontendMgr {
  #[inline]
  pub(crate) fn new() -> Self {
    let frontends = DashMap::with_capacity_and_hasher(64, AHasher::default());
    let frontend_mgr = FrontendMgr { frontends };
    frontend_mgr.initialize();
    frontend_mgr
  }

  #[inline]
  pub fn activate(&self, id: &NodeId) {
    if let Some(mut frontend) = self.frontends.get_mut(id) {
      frontend.active_at = Utc::now().timestamp() as u32;
    }
  }

  #[inline]
  pub fn get<'a>(&'a self, id: &NodeId) -> Option<FrontendRef<'a>> {
    if let Some(frontend) = self.frontends.get(id) {
      Some(frontend)
    } else {
      None
    }
  }

  #[inline]
  pub fn pick<'a>(&'a self) -> Option<FrontendRefMulti<'a>> {
    let mut rng = thread_rng();
    let index = rng.gen_range(0..self.frontends.len());
    self.frontends.iter().nth(index).map(|frontend| frontend)
  }

  #[inline]
  pub fn iter<'a>(&'a self) -> FrontendIter<'a> {
    self.frontends.iter()
  }

  #[inline]
  fn initialize(&self) {
    CONFIG.frontend_mgr.frontends.iter().for_each(|frontend_config| {
      let frontend = Frontend::new(
        frontend_config.domain.clone(),
        frontend_config.public_ip,
        frontend_config.private_ip,
        frontend_config.http_port,
        frontend_config.https_port,
      );
      self.frontends.insert(frontend.id.clone(), frontend.clone());
    });
  }
}

pub static FRONTEND_MGR: Lazy<FrontendMgr> = Lazy::new(|| FrontendMgr::new());

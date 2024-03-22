use std::{fmt::Debug, net::IpAddr};

use ahash::RandomState as AHasher;
use dashmap::{
  iter::Iter,
  mapref::{multiple::RefMulti, one::Ref},
};

pub mod backend_mgr;
pub mod frontend_mgr;
pub mod service_mgr;

pub type NodeId = String;

pub fn build_node_id(ip: IpAddr, port: u32) -> NodeId {
  format!("{}:{}", ip, port)
}

#[derive(Debug, Clone, Copy)]
pub enum NodeType {
  Unknown,
  Frontend,
  Backend,
  Service,
}

pub trait Node: Clone + Debug {
  fn id(&self) -> &NodeId;
  fn active_at(&self) -> u32;
  fn set_active_at(&mut self, active_at: u32);
}

pub type NodeRef<'a, N> = Ref<'a, NodeId, N, AHasher>;
pub type NodeRefMulti<'a, N> = RefMulti<'a, NodeId, N, AHasher>;
pub type NodeIter<'a, N> = Iter<'a, NodeId, N, AHasher>;

pub use backend_mgr::*;
pub use frontend_mgr::*;
pub use service_mgr::*;

use std::{borrow::Borrow, fmt::Debug, net::IpAddr};

use ahash::RandomState as AHasher;
use bytes::{Bytes, BytesMut};
use chrono::prelude::*;
use dashmap::{
  mapref::{entry::Entry, one::Ref},
  DashMap,
};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use seriesdb::{
  coder::Coder,
  prelude::Db,
  table::{NormalTable, Table, TableEnhanced},
};

use super::{build_node_id, Node, NodeId};
use crate::{config::CONFIG, db::DB};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Service {
  pub(crate) id: NodeId,
  pub(crate) private_ip: IpAddr,
  pub(crate) http_port: u32,
  pub(crate) active_at: u32,
}

impl Node for Service {
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

impl Service {
  pub fn new(private_ip: IpAddr, http_port: u32) -> Self {
    Service {
      id: build_node_id(private_ip, http_port),
      http_port,
      private_ip,
      active_at: Utc::now().timestamp() as u32,
    }
  }

  #[inline]
  pub fn is_healthy(&self) -> bool {
    if Utc::now().timestamp() as u32 - self.active_at > CONFIG.service_mgr.unhealthy_threshold {
      false
    } else {
      true
    }
  }
}

pub struct ServiceCoder;

impl Coder<NodeId, Service> for ServiceCoder {
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
  fn encode_value<V: Borrow<Service>>(value: V) -> Self::EncodedValue {
    bincode::serialize(value.borrow()).unwrap().into()
  }

  #[inline(always)]
  fn decode_value(value: &[u8]) -> Service {
    bincode::deserialize(value).unwrap()
  }
}

pub type ServiceRef<'a> = Ref<'a, NodeId, Service, AHasher>;
type ServiceStore = TableEnhanced<NormalTable, NodeId, Service, ServiceCoder>;

pub struct ServiceMgr {
  pub(crate) cache: DashMap<NodeId, Service, AHasher>,
  pub(crate) service_store: ServiceStore,
}

impl ServiceMgr {
  #[inline]
  pub(crate) fn new(service_store: ServiceStore) -> Self {
    let cache = DashMap::with_capacity_and_hasher(64, AHasher::default());
    let service_mgr = ServiceMgr { cache, service_store };
    service_mgr.recover();
    service_mgr
  }

  #[inline]
  pub fn add(&self, service: Service) {
    let id_bytes = <ServiceCoder as Coder<NodeId, Service>>::encode_key(&service.id);
    let service_bytes = <ServiceCoder as Coder<NodeId, Service>>::encode_value(&service);
    self.cache.insert(service.id.clone(), service);
    self
      .service_store
      .raw()
      .put(id_bytes, service_bytes)
      .unwrap_or_else(|err| log::warn!("Failed to add service: err: {:?}", err));
  }

  #[inline]
  pub fn remove(&self, id: &NodeId) {
    self.cache.remove(id);
    self
      .service_store
      .delete(id)
      .unwrap_or_else(|err| log::warn!("Failed to remove service: err: {:?}", err));
  }

  #[inline]
  pub fn activate(&self, id: &NodeId) {
    if let Some(mut service) = self.cache.get_mut(id) {
      let now = Utc::now().timestamp() as u32;
      service.active_at = now;
      self
        .service_store
        .put(id, &*service)
        .unwrap_or_else(|err| log::warn!("Failed to activate node: err: {:?}", err));
    }
  }

  #[inline]
  pub fn get<'a>(&'a self, id: &NodeId) -> Option<ServiceRef<'a>> {
    match self.cache.entry(id.clone()) {
      Entry::Occupied(entry) => {
        let service = entry.get();
        if Utc::now().timestamp() as u32 - service.active_at > CONFIG.service_mgr.stale_threshold {
          entry.remove();
          self
            .service_store
            .delete(id)
            .unwrap_or_else(|err| log::warn!("Failed to remove service: err: {:?}", err));
          None
        } else {
          Some(entry.into_ref().downgrade())
        }
      }
      Entry::Vacant(_) => None,
    }
  }

  #[inline]
  fn recover(&self) {
    let mut cursor = self.service_store.new_cursor();
    cursor.seek_to_first();
    while cursor.is_valid() {
      let id = cursor.key().unwrap();
      let service: Service = cursor.value().unwrap();
      self.cache.insert(id, service);
      cursor.next();
    }
  }
}

pub static SERVICE_MGR: Lazy<ServiceMgr> = Lazy::new(|| {
  ServiceMgr::new(
    DB.open_table("node_mgr.service_mgr.services")
      .unwrap()
      .enhance::<NodeId, Service, ServiceCoder>(),
  )
});

#[cfg(test)]
mod tests {
  use std::net::{IpAddr, Ipv4Addr};
  use std::sync::Arc;

  use seriesdb::prelude::{Db, NormalDb, Options};

  use super::*;

  #[test]
  fn test_basic() {
    let db = Arc::new(NormalDb::open("data/test_basic", &mut Options::new()).unwrap());
    db.truncate_table("test_services").unwrap();
    let table = db.open_table("test_services").unwrap().enhance();

    let service_mgr = ServiceMgr::new(table);

    let ip = "127.0.0.1".parse::<Ipv4Addr>().unwrap();
    let port = 10000;
    let input_service = Service::new(IpAddr::V4(ip), port);

    let id = input_service.id().clone();
    let output_service = service_mgr.get(&id);
    assert!(output_service.is_none());
    service_mgr.add(input_service);
    let output_node = service_mgr.get(&id);
    assert!(output_node.is_some());
  }
}

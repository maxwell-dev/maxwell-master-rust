use std::{
  borrow::Borrow,
  fmt::Debug,
  net::IpAddr,
  sync::atomic::{AtomicU32, Ordering},
};

use ahash::RandomState as AHasher;
use bytes::{Bytes, BytesMut};
use chrono::Utc;
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

use super::{Node, NodeId, NodeIter};
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
  pub fn new(id: String, private_ip: IpAddr, http_port: u32) -> Self {
    Service { id, http_port, private_ip, active_at: Utc::now().timestamp() as u32 }
  }

  #[inline]
  pub fn private_endpoint(&self) -> String {
    format!("{}:{}", self.private_ip, self.http_port)
  }

  #[inline]
  pub fn is_healthy(&self) -> bool {
    if Utc::now().timestamp() as u32 - self.active_at > CONFIG.service_mgr.unhealthy_threshold {
      false
    } else {
      true
    }
  }

  #[inline]
  pub fn is_stale(&self) -> bool {
    if Utc::now().timestamp() as u32 - self.active_at > CONFIG.service_mgr.stale_threshold {
      true
    } else {
      false
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

pub type ServiceRef<'a> = Ref<'a, NodeId, Service>;
type ServiceStore = TableEnhanced<NormalTable, NodeId, Service, ServiceCoder>;
pub type ServiceIter<'a> = NodeIter<'a, Service>;

pub struct ServiceMgr {
  cache: DashMap<NodeId, Service, AHasher>,
  service_store: ServiceStore,
  version: AtomicU32,
}

impl ServiceMgr {
  #[inline]
  pub(crate) fn new(service_store: ServiceStore) -> Self {
    let cache = DashMap::with_capacity_and_hasher(64, AHasher::default());
    let service_mgr = ServiceMgr {
      cache,
      service_store,
      version: AtomicU32::new(crc32fast::hash(
        format!("{}", Utc::now().timestamp_millis()).as_bytes(),
      )),
    };
    service_mgr.recover();
    service_mgr
  }

  #[inline]
  pub fn add(&self, service: Service) {
    let id_bytes = <ServiceCoder as Coder<NodeId, Service>>::encode_key(&service.id);
    let service_bytes = <ServiceCoder as Coder<NodeId, Service>>::encode_value(&service);
    match self.cache.entry(service.id.clone()) {
      Entry::Occupied(mut entry) => {
        let curr_service = entry.get();
        let changed = if service.private_ip != curr_service.private_ip
          || service.http_port != curr_service.http_port
        {
          log::info!(
            "The service's private endpoint was changed: current: {:?}, new: {:?}",
            curr_service.private_endpoint(),
            service.private_endpoint()
          );
          true
        } else {
          log::debug!("The service is the same, no need to update version.");
          false
        };
        entry.insert(service);
        self
          .service_store
          .raw()
          .put(id_bytes, service_bytes)
          .unwrap_or_else(|err| log::warn!("Failed to add service: err: {:?}", err));
        if changed {
          self.update_version();
        }
      }
      Entry::Vacant(entry) => {
        log::debug!("Adding service: {:?}", service);
        entry.insert(service);
        self
          .service_store
          .raw()
          .put(id_bytes, service_bytes)
          .unwrap_or_else(|err| log::warn!("Failed to add service: err: {:?}", err));
        self.update_version();
      }
    }
  }

  #[inline]
  pub fn remove(&self, id: &NodeId) {
    if self.cache.remove(id).is_some() {
      self
        .service_store
        .delete(id)
        .unwrap_or_else(|err| log::warn!("Failed to remove service: err: {:?}", err));
      self.update_version();
    }
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
        if service.is_stale() {
          entry.remove();
          self
            .service_store
            .delete(id)
            .unwrap_or_else(|err| log::warn!("Failed to remove service: err: {:?}", err));
          self.update_version();
          None
        } else {
          Some(entry.into_ref().downgrade())
        }
      }
      Entry::Vacant(_) => None,
    }
  }

  #[allow(dead_code)]
  #[inline]
  pub fn iter<'a>(&'a self) -> ServiceIter<'a> {
    self.cache.iter()
  }

  #[inline]
  pub fn version(&self) -> u32 {
    self.version.load(Ordering::SeqCst)
  }

  #[inline]
  fn update_version(&self) {
    self.version.fetch_add(1, Ordering::SeqCst);
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

    let id = "service-0";
    let ip = "127.0.0.1".parse::<Ipv4Addr>().unwrap();
    let port = 10000;
    let input_service = Service::new(id.to_owned(), IpAddr::V4(ip), port);

    let id = input_service.id().clone();
    let output_service = service_mgr.get(&id);
    assert!(output_service.is_none());
    service_mgr.add(input_service);
    let output_node = service_mgr.get(&id);
    assert!(output_node.is_some());
  }
}

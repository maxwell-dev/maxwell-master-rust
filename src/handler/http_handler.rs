use std::net::{IpAddr, SocketAddr};

use actix_web::HttpRequest;
use ahash::HashMap;
use maxwell_protocol::{self, *};
use serde::Serialize;

use crate::{
  node_mgr::*,
  route_mgr::{PathSet, ROUTE_MGR},
};

#[derive(Debug, Serialize)]
pub struct AssignFrontendRep {
  code: i32,
  #[serde(skip_serializing_if = "Option::is_none")]
  desc: Option<String>,
  #[serde(skip_serializing_if = "Option::is_none")]
  endpoint: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct GetFrontendsRep {
  code: i32,
  #[serde(skip_serializing_if = "Option::is_none")]
  desc: Option<String>,
  endpoints: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetRoutesRep {
  code: i32,
  #[serde(skip_serializing_if = "Option::is_none")]
  desc: Option<String>,
  ws_route_groups: Vec<RouteGroup>,
  get_route_groups: Vec<RouteGroup>,
  post_route_groups: Vec<RouteGroup>,
  put_route_groups: Vec<RouteGroup>,
  patch_route_groups: Vec<RouteGroup>,
  delete_route_groups: Vec<RouteGroup>,
  head_route_groups: Vec<RouteGroup>,
  options_route_groups: Vec<RouteGroup>,
  trace_route_groups: Vec<RouteGroup>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AddrType {
  Loopback,
  Private,
  Public,
}

pub struct HttpHandler {
  addr_type: AddrType,
  is_https: bool,
}

impl HttpHandler {
  #[inline]
  pub fn new(req: &HttpRequest) -> Self {
    Self {
      addr_type: if let Some(peer_addr) = &req.peer_addr() {
        Self::detect_addr_type(peer_addr)
      } else {
        AddrType::Public
      },
      is_https: req.connection_info().scheme() == "https",
    }
  }

  #[inline]
  pub fn pick_frontend(&self) -> AssignFrontendRep {
    if let Some(frontend) = FRONTEND_MGR.pick() {
      AssignFrontendRep {
        code: ErrorCode::Ok as i32,
        desc: None,
        endpoint: Some(self.build_endpoint(&frontend)),
      }
    } else {
      log::error!("Failed to pick an available frontend.");

      AssignFrontendRep {
        code: ErrorCode::FailedToPickFrontend as i32,
        desc: Some(format!("Failed to pick an available frontend.")),
        endpoint: None,
      }
    }
  }

  #[inline]
  pub fn pick_frontends(&self) -> GetFrontendsRep {
    let mut endpoints = vec![];
    for frontend in FRONTEND_MGR.iter() {
      endpoints.push(self.build_endpoint(&frontend));
    }
    GetFrontendsRep { code: ErrorCode::Ok as i32, desc: None, endpoints }
  }

  #[inline]
  pub fn get_routes(&self) -> GetRoutesRep {
    let mut stale_services = vec![];

    let mut ws_route_groups = HashMap::default();
    let mut get_route_groups = HashMap::default();
    let mut post_route_groups = HashMap::default();
    let mut put_route_groups = HashMap::default();
    let mut patch_route_groups = HashMap::default();
    let mut delete_route_groups = HashMap::default();
    let mut head_route_groups = HashMap::default();
    let mut options_route_groups = HashMap::default();
    let mut trace_route_groups = HashMap::default();

    for reverse_route_group in ROUTE_MGR.reverse_route_group_iter() {
      let pb = reverse_route_group.value();
      let service_id = reverse_route_group.key();
      let (endpoint, is_healthy) = match SERVICE_MGR.get(service_id) {
        Some(service) => (service.private_endpoint(), service.is_healthy()),
        None => {
          log::warn!("Found a stale service: id: {:?}", service_id);
          stale_services.push(service_id.clone());
          continue;
        }
      };

      Self::build_route_groups(&mut ws_route_groups, &pb.ws_paths, &endpoint, is_healthy);
      Self::build_route_groups(&mut get_route_groups, &pb.get_paths, &endpoint, is_healthy);
      Self::build_route_groups(&mut post_route_groups, &pb.post_paths, &endpoint, is_healthy);
      Self::build_route_groups(&mut put_route_groups, &pb.put_paths, &endpoint, is_healthy);
      Self::build_route_groups(&mut patch_route_groups, &pb.patch_paths, &endpoint, is_healthy);
      Self::build_route_groups(&mut delete_route_groups, &pb.delete_paths, &endpoint, is_healthy);
      Self::build_route_groups(&mut head_route_groups, &pb.head_paths, &endpoint, is_healthy);
      Self::build_route_groups(&mut options_route_groups, &pb.options_paths, &endpoint, is_healthy);
      Self::build_route_groups(&mut trace_route_groups, &pb.trace_paths, &endpoint, is_healthy);
    }

    for service_id in &stale_services {
      SERVICE_MGR.remove(service_id);
      ROUTE_MGR.remove_reverse_route_group(service_id);
    }

    GetRoutesRep {
      code: ErrorCode::Ok as i32,
      desc: None,
      ws_route_groups: ws_route_groups.values().cloned().collect(),
      get_route_groups: get_route_groups.values().cloned().collect(),
      post_route_groups: post_route_groups.values().cloned().collect(),
      put_route_groups: put_route_groups.values().cloned().collect(),
      patch_route_groups: patch_route_groups.values().cloned().collect(),
      delete_route_groups: delete_route_groups.values().cloned().collect(),
      head_route_groups: head_route_groups.values().cloned().collect(),
      options_route_groups: options_route_groups.values().cloned().collect(),
      trace_route_groups: trace_route_groups.values().cloned().collect(),
    }
  }

  #[inline]
  fn detect_addr_type(addr: &SocketAddr) -> AddrType {
    match addr.ip() {
      IpAddr::V4(ip) => {
        if ip.is_loopback() {
          AddrType::Loopback
        } else if ip.is_private() {
          AddrType::Private
        } else {
          AddrType::Public
        }
      }
      IpAddr::V6(_) => AddrType::Public,
    }
  }

  #[inline]
  fn build_endpoint(&self, frontend: &Frontend) -> String {
    if self.addr_type == AddrType::Loopback {
      if self.is_https {
        format!("{}:{}", frontend.domain, frontend.https_port)
      } else {
        format!("{}:{}", frontend.private_ip, frontend.http_port)
      }
    } else if self.addr_type == AddrType::Private {
      format!("{}:{}", frontend.private_ip, frontend.http_port)
    } else {
      if self.is_https {
        format!("{}:{}", frontend.domain, frontend.https_port)
      } else {
        format!("{}:{}", frontend.public_ip, frontend.http_port)
      }
    }
  }

  #[inline(always)]
  fn build_route_groups(
    route_groups_map: &mut HashMap<String, RouteGroup>, paths: &PathSet, endpoint: &String,
    is_healthy: bool,
  ) {
    for path in paths {
      let ws_route_group = route_groups_map.entry(path.to_owned()).or_insert_with(|| RouteGroup {
        path: path.clone(),
        healthy_endpoints: Vec::new(),
        unhealthy_endpoints: Vec::new(),
      });
      if is_healthy {
        ws_route_group.healthy_endpoints.push(endpoint.clone());
      } else {
        ws_route_group.unhealthy_endpoints.push(endpoint.clone());
      }
    }
  }
}

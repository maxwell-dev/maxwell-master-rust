use std::net::{IpAddr, SocketAddr};

use actix_web::HttpRequest;
use maxwell_protocol::{self, *};
use serde::Serialize;

use crate::node_mgr::*;

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
  fn detect_addr_type(peer_addr: &SocketAddr) -> AddrType {
    match peer_addr.ip() {
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
}

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

pub struct HttpHandler {
  is_lan: bool,
  is_https: bool,
}

impl HttpHandler {
  #[inline]
  pub fn new(req: &HttpRequest) -> Self {
    Self {
      is_lan: if let Some(peer_addr) = &req.peer_addr() { Self::is_lan(peer_addr) } else { false },
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
  fn is_lan(peer_addr: &SocketAddr) -> bool {
    match peer_addr.ip() {
      IpAddr::V4(ip) => {
        if ip.is_private() {
          true
        } else {
          false
        }
      }
      IpAddr::V6(_) => false,
    }
  }

  #[inline]
  fn build_endpoint(&self, frontend: &Frontend) -> String {
    let ip = if self.is_lan { frontend.private_ip } else { frontend.public_ip };
    let port = if self.is_https { frontend.https_port } else { frontend.http_port };
    format!("{}:{}", ip, port)
  }
}

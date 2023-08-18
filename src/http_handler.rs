use std::net::{IpAddr, SocketAddr};

use actix_web::HttpRequest;
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
  peer_addr: SocketAddr,
}

impl HttpHandler {
  pub fn new(req: &HttpRequest) -> Self {
    Self { peer_addr: req.peer_addr().unwrap() }
  }

  pub fn assign_frontend(&self) -> AssignFrontendRep {
    if let Some(frontend) = FRONTEND_MGR.next() {
      let ip = if self.is_lan() { frontend.private_ip } else { frontend.public_ip };
      AssignFrontendRep {
        code: 0,
        desc: None,
        endpoint: Some(format!("{}:{}", ip, frontend.http_port)),
      }
    } else {
      AssignFrontendRep {
        code: 1,
        desc: Some(format!("Failed to find an available frontend.")),
        endpoint: None,
      }
    }
  }

  pub fn get_frontends(&self) -> GetFrontendsRep {
    let mut endpoints = vec![];
    for frontend in FRONTEND_MGR.iter() {
      endpoints.push(format!("{}:{}", frontend.public_ip, frontend.http_port))
    }
    GetFrontendsRep { code: 0, desc: None, endpoints }
  }

  #[inline]
  fn is_lan(&self) -> bool {
    match self.peer_addr.ip() {
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
}

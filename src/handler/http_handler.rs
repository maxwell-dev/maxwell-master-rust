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
  peer_addr: SocketAddr,
}

impl HttpHandler {
  pub fn new(req: &HttpRequest) -> Self {
    Self { peer_addr: req.peer_addr().unwrap() }
  }

  pub fn pick_frontend(&self) -> AssignFrontendRep {
    if let Some(frontend) = FRONTEND_MGR.pick() {
      let ip = if self.is_lan() { frontend.private_ip } else { frontend.public_ip };
      AssignFrontendRep {
        code: ErrorCode::Ok as i32,
        desc: None,
        endpoint: Some(format!("{}:{}", ip, frontend.http_port)),
      }
    } else {
      AssignFrontendRep {
        code: ErrorCode::FailedToPickFrontend as i32,
        desc: Some(format!("Failed to pick an available frontend.")),
        endpoint: None,
      }
    }
  }

  pub fn pick_frontends(&self) -> GetFrontendsRep {
    let is_lan = self.is_lan();
    let mut endpoints = vec![];
    for frontend in FRONTEND_MGR.iter() {
      if is_lan {
        endpoints.push(format!("{}:{}", frontend.private_ip, frontend.http_port))
      } else {
        endpoints.push(format!("{}:{}", frontend.public_ip, frontend.http_port))
      }
    }
    GetFrontendsRep { code: ErrorCode::Ok as i32, desc: None, endpoints }
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

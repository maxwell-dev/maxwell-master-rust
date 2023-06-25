use std::{
  cell::{Cell, RefCell},
  net::{IpAddr, SocketAddr},
  rc::Rc,
  sync::atomic::{AtomicU32, Ordering},
};

use actix::{prelude::*, Actor};
use actix_web::HttpRequest;
use actix_web_actors::ws;
use ahash::HashMap;
use chrono::Utc;
use maxwell_protocol::{self, *};

use crate::route_mgr::*;
use crate::{node_mgr::*, topic_mgr::TOPIC_MGR};

static ID_SEED: AtomicU32 = AtomicU32::new(1);

fn next_id() -> u32 {
  ID_SEED.fetch_add(1, Ordering::Relaxed)
}

struct HandlerInner {
  id: u32,
  peer_addr: SocketAddr,
  node_type: Cell<NodeType>,
  node_id: RefCell<Option<NodeId>>,
}

impl HandlerInner {
  fn new(req: &HttpRequest) -> Self {
    HandlerInner {
      id: next_id(),
      peer_addr: req.peer_addr().unwrap(),
      node_type: Cell::new(NodeType::Unknown),
      node_id: RefCell::new(None),
    }
  }

  async fn handle_external_msg(self: Rc<Self>, protocol_msg: ProtocolMsg) -> ProtocolMsg {
    log::debug!("received external msg: {:?}", protocol_msg);
    match protocol_msg {
      ProtocolMsg::PingReq(req) => self.handle_ping_req(req),
      ProtocolMsg::RegisterFrontendReq(req) => self.handle_register_frontend_req(req),
      ProtocolMsg::RegisterBackendReq(req) => self.handle_register_backend_req(req),
      ProtocolMsg::RegisterServerReq(req) => self.handle_register_server_req(req),
      ProtocolMsg::AddRoutesReq(req) => self.handle_add_routes_req(req),
      ProtocolMsg::GetRoutesReq(req) => self.handle_get_routes_req(req),
      ProtocolMsg::AssignFrontendReq(req) => self.handle_assign_frontend_req(req),
      ProtocolMsg::LocateTopicReq(req) => self.handle_locate_topic_req(req),
      ProtocolMsg::ResolveIpReq(req) => self.handle_resolve_ip_req(req),
      _ => maxwell_protocol::ErrorRep {
        code: 1,
        desc: format!("Received unknown msg: {:?}", protocol_msg),
        r#ref: get_ref(&protocol_msg),
      }
      .into_enum(),
    }
  }

  async fn handle_internal_msg(self: Rc<Self>, protocol_msg: ProtocolMsg) -> ProtocolMsg {
    log::debug!("received internal msg: {:?}", protocol_msg);
    match &protocol_msg {
      _ => maxwell_protocol::ErrorRep {
        code: 1,
        desc: format!("Received unknown msg: {:?}", protocol_msg),
        r#ref: 0,
      }
      .into_enum(),
    }
  }

  #[inline(always)]
  fn handle_ping_req(
    self: Rc<Self>, req: maxwell_protocol::PingReq,
  ) -> maxwell_protocol::ProtocolMsg {
    if let Some(node_id) = self.node_id.borrow().as_ref() {
      match self.node_type.get() {
        NodeType::Frontend => FRONTEND_MGR.activate(node_id),
        NodeType::Backend => BACKEND_MGR.activate(node_id),
        NodeType::Server => SERVER_MGR.activate(node_id),
        _ => {}
      }
    }
    maxwell_protocol::PingRep { r#ref: req.r#ref }.into_enum()
  }

  #[inline(always)]
  fn handle_register_frontend_req(
    self: Rc<Self>, req: maxwell_protocol::RegisterFrontendReq,
  ) -> maxwell_protocol::ProtocolMsg {
    self.node_type.set(NodeType::Frontend);
    let frontend = Frontend::new(
      req.public_ip.parse().unwrap(),
      self.peer_addr.ip(),
      req.http_port,
      req.https_port,
    );
    *self.node_id.borrow_mut() = Some(frontend.id().clone());
    FRONTEND_MGR.add(frontend);
    maxwell_protocol::RegisterFrontendRep { r#ref: req.r#ref }.into_enum()
  }

  #[inline(always)]
  fn handle_register_backend_req(
    self: Rc<Self>, req: maxwell_protocol::RegisterBackendReq,
  ) -> maxwell_protocol::ProtocolMsg {
    self.node_type.set(NodeType::Backend);
    let backend = Backend::new(self.peer_addr.ip(), req.http_port);
    *self.node_id.borrow_mut() = Some(backend.id().clone());
    BACKEND_MGR.add(backend);
    maxwell_protocol::RegisterBackendRep { r#ref: req.r#ref }.into_enum()
  }

  #[inline(always)]
  fn handle_register_server_req(
    self: Rc<Self>, req: maxwell_protocol::RegisterServerReq,
  ) -> maxwell_protocol::ProtocolMsg {
    self.node_type.set(NodeType::Server);
    let server = Server::new(self.peer_addr.ip(), req.http_port);
    *self.node_id.borrow_mut() = Some(server.id().clone());
    SERVER_MGR.add(server);
    maxwell_protocol::RegisterServerRep { r#ref: req.r#ref }.into_enum()
  }

  #[inline(always)]
  fn handle_add_routes_req(
    self: Rc<Self>, req: maxwell_protocol::AddRoutesReq,
  ) -> maxwell_protocol::ProtocolMsg {
    let server_id = self.node_id.borrow().as_ref().unwrap_or(&"unknown".to_owned()).clone();
    ROUTE_MGR.add_reverse_route_group(server_id, req.paths);
    maxwell_protocol::AddRoutesRep { r#ref: req.r#ref }.into_enum()
  }

  #[inline(always)]
  fn handle_get_routes_req(
    self: Rc<Self>, req: maxwell_protocol::GetRoutesReq,
  ) -> maxwell_protocol::ProtocolMsg {
    let mut route_groups = HashMap::default();

    for reverse_route_group in ROUTE_MGR.reverse_route_group_iter() {
      let endpoint = reverse_route_group.key();
      let path_set = reverse_route_group.value();

      let is_healthy = match SERVER_MGR.get(endpoint) {
        Some(server) => Utc::now().timestamp() as u32 - server.active_at() < 60,
        None => false,
      };

      for path in path_set {
        let route_group = route_groups.entry(path.to_owned()).or_insert_with(|| RouteGroup {
          path: path.clone(),
          healthy_endpoints: Vec::new(),
          unhealthy_endpoints: Vec::new(),
        });
        if is_healthy {
          route_group.healthy_endpoints.push(endpoint.clone());
        } else {
          route_group.unhealthy_endpoints.push(endpoint.clone());
        }
      }
    }

    maxwell_protocol::GetRoutesRep {
      route_groups: route_groups.values().cloned().collect(),
      r#ref: req.r#ref,
    }
    .into_enum()
  }

  #[inline(always)]
  fn handle_assign_frontend_req(
    self: Rc<Self>, req: maxwell_protocol::AssignFrontendReq,
  ) -> maxwell_protocol::ProtocolMsg {
    if let Some(frontend) = FRONTEND_MGR.next() {
      let ip = match self.peer_addr.ip() {
        IpAddr::V4(ip) => {
          if ip.is_private() {
            frontend.private_ip
          } else {
            frontend.public_ip
          }
        }
        IpAddr::V6(_) => frontend.public_ip,
      };
      maxwell_protocol::AssignFrontendRep {
        endpoint: format!("{}:{}", ip, frontend.http_port),
        r#ref: req.r#ref,
      }
      .into_enum()
    } else {
      maxwell_protocol::ErrorRep {
        code: 1,
        desc: format!("Failed to find an available frontend."),
        r#ref: req.r#ref,
      }
      .into_enum()
    }
  }

  #[inline(always)]
  fn handle_locate_topic_req(
    self: Rc<Self>, req: maxwell_protocol::LocateTopicReq,
  ) -> maxwell_protocol::ProtocolMsg {
    match TOPIC_MGR.locate(&req.topic) {
      Ok(Some(backend_id)) => {
        if let Some(backend) = BACKEND_MGR.get(&backend_id) {
          maxwell_protocol::LocateTopicRep {
            endpoint: format!("{}:{}", backend.private_ip, backend.http_port),
            r#ref: req.r#ref,
          }
          .into_enum()
        } else {
          maxwell_protocol::ErrorRep {
            code: 1,
            desc: format!(
              "Failed to find the backend: topic: {}, backend_id: {}",
              req.topic, backend_id
            ),
            r#ref: req.r#ref,
          }
          .into_enum()
        }
      }
      Ok(None) => {
        if let Some(backend) = BACKEND_MGR.next() {
          match TOPIC_MGR.assign(req.topic.clone(), backend.id().clone()) {
            Ok(()) => maxwell_protocol::LocateTopicRep {
              endpoint: format!("{}:{}", backend.private_ip, backend.http_port),
              r#ref: req.r#ref,
            }
            .into_enum(),
            Err(err) => {
              return maxwell_protocol::ErrorRep {
                code: 1,
                desc: format!("Failed to assign topic: {}, err: {}", req.topic, err),
                r#ref: req.r#ref,
              }
              .into_enum()
            }
          }
        } else {
          maxwell_protocol::ErrorRep {
            code: 1,
            desc: format!("Failed to find an available backend: topic: {}", req.topic),
            r#ref: req.r#ref,
          }
          .into_enum()
        }
      }
      Err(err) => maxwell_protocol::ErrorRep {
        code: 1,
        desc: format!("Failed to locate topic: {}, err: {}", req.topic, err),
        r#ref: req.r#ref,
      }
      .into_enum(),
    }
  }

  #[inline(always)]
  fn handle_resolve_ip_req(
    self: Rc<Self>, req: maxwell_protocol::ResolveIpReq,
  ) -> maxwell_protocol::ProtocolMsg {
    maxwell_protocol::ResolveIpRep { ip: self.peer_addr.ip().to_string(), r#ref: req.r#ref }
      .into_enum()
  }
}

pub struct Handler {
  inner: Rc<HandlerInner>,
}

impl Actor for Handler {
  type Context = ws::WebsocketContext<Self>;

  fn started(&mut self, _ctx: &mut Self::Context) {
    log::info!("Handler actor started: id: {:?}", self.inner.id);
  }

  fn stopping(&mut self, _ctx: &mut Self::Context) -> Running {
    log::info!("Handler actor stopping: id: {:?}", self.inner.id);
    Running::Stop
  }

  fn stopped(&mut self, _ctx: &mut Self::Context) {
    log::info!("Handler actor stopped: id: {:?}", self.inner.id);
  }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for Handler {
  fn handle(&mut self, ws_msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
    match ws_msg {
      Ok(ws::Message::Ping(ws_msg)) => {
        if let Some(node_id) = self.inner.node_id.borrow().as_ref() {
          match self.inner.node_type.get() {
            NodeType::Frontend => FRONTEND_MGR.activate(node_id),
            NodeType::Backend => BACKEND_MGR.activate(node_id),
            NodeType::Server => SERVER_MGR.activate(node_id),
            _ => {}
          }
        }
        ctx.pong(&ws_msg);
      }
      Ok(ws::Message::Pong(_)) => (),
      Ok(ws::Message::Text(_)) => (),
      Ok(ws::Message::Binary(bin)) => {
        let inner = self.inner.clone();
        async move {
          let res = maxwell_protocol::decode(&bin.into());
          match res {
            Ok(req) => Ok(inner.handle_external_msg(req).await),
            Err(err) => Err(err),
          }
        }
        .into_actor(self)
        .map(move |res, _act, ctx| match res {
          Ok(msg) => {
            if msg.is_some() {
              ctx.binary(maxwell_protocol::encode(&msg));
            }
          }
          Err(err) => log::error!("Failed to decode msg: {:?}", err),
        })
        .spawn(ctx);
      }
      Ok(ws::Message::Close(_)) => ctx.stop(),
      _ => log::error!("Received unknown msg: {:?}", ws_msg),
    }
  }
}

impl actix::Handler<ProtocolMsg> for Handler {
  type Result = Result<ProtocolMsg, HandleError<ProtocolMsg>>;

  fn handle(&mut self, protocol_msg: ProtocolMsg, ctx: &mut Self::Context) -> Self::Result {
    let inner = self.inner.clone();
    async move { inner.handle_internal_msg(protocol_msg).await }
      .into_actor(self)
      .map(move |res, _act, ctx| {
        if res.is_some() {
          ctx.binary(maxwell_protocol::encode(&res));
        }
      })
      .spawn(ctx);
    Ok(ProtocolMsg::None)
  }
}

impl Handler {
  pub fn new(req: &HttpRequest) -> Self {
    Self { inner: Rc::new(HandlerInner::new(req)) }
  }
}

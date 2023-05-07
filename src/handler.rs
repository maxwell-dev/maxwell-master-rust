use std::{
  cell::{Cell, RefCell},
  net::SocketAddr,
  rc::Rc,
  sync::atomic::{AtomicU32, Ordering},
};

use actix::{prelude::*, Actor};
use actix_web::HttpRequest;
use actix_web_actors::ws;
use ahash::HashMap;
use chrono::Utc;
use maxwell_protocol::{self, SendError, *};

use crate::node_mgr::*;
use crate::route_mgr::*;

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
    log::info!("received external msg: {:?}", protocol_msg);
    match protocol_msg {
      ProtocolMsg::PingReq(req) => {
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
      ProtocolMsg::RegisterFrontendReq(req) => {
        self.node_type.set(NodeType::Frontend);
        let frontend = Frontend::new(
          req.public_ip.parse().unwrap(),
          self.peer_addr.ip(),
          req.http_port,
          req.https_port,
        );
        *self.node_id.borrow_mut() = Some(frontend.id().clone());
        FRONTEND_MGR.add(frontend);
        maxwell_protocol::RegisterServerRep { r#ref: req.r#ref }.into_enum()
      }
      ProtocolMsg::RegisterBackendReq(req) => {
        self.node_type.set(NodeType::Backend);
        let backend = Backend::new(self.peer_addr.ip(), req.http_port);
        *self.node_id.borrow_mut() = Some(backend.id().clone());
        BACKEND_MGR.add(backend);
        maxwell_protocol::RegisterServerRep { r#ref: req.r#ref }.into_enum()
      }
      ProtocolMsg::RegisterServerReq(req) => {
        self.node_type.set(NodeType::Server);
        let server = Server::new(self.peer_addr.ip(), req.http_port);
        *self.node_id.borrow_mut() = Some(server.id().clone());
        SERVER_MGR.add(server);
        maxwell_protocol::RegisterServerRep { r#ref: req.r#ref }.into_enum()
      }
      ProtocolMsg::AddRoutesReq(req) => {
        let server_id = self.node_id.borrow().as_ref().unwrap_or(&"unknown".to_owned()).clone();
        ROUTE_MGR.add_reverse_route_group(server_id, req.paths);
        maxwell_protocol::RegisterServerRep { r#ref: req.r#ref }.into_enum()
      }
      ProtocolMsg::GetRoutesReq(req) => {
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
      ProtocolMsg::AssignFrontendReq(req) => maxwell_protocol::AssignFrontendRep {
        endpoint: format!("127.0.0.1:10000"),
        r#ref: req.r#ref,
      }
      .into_enum(),
      ProtocolMsg::LocateTopicReq(req) => {
        maxwell_protocol::LocateTopicRep { endpoint: format!("127.0.0.1:20000"), r#ref: req.r#ref }
          .into_enum()
      }
      ProtocolMsg::ResolveIpReq(req) => {
        maxwell_protocol::ResolveIpRep { ip: self.peer_addr.ip().to_string(), r#ref: req.r#ref }
          .into_enum()
      }
      _ => maxwell_protocol::ErrorRep {
        code: 1,
        desc: format!("Received unknown msg: {:?}", protocol_msg),
        r#ref: get_ref(&protocol_msg),
      }
      .into_enum(),
    }
  }

  async fn handle_internal_msg(self: Rc<Self>, protocol_msg: ProtocolMsg) -> ProtocolMsg {
    log::info!("received internal msg: {:?}", protocol_msg);
    match &protocol_msg {
      _ => maxwell_protocol::ErrorRep {
        code: 1,
        desc: format!("Received unknown msg: {:?}", protocol_msg),
        r#ref: 0,
      }
      .into_enum(),
    }
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
  type Result = Result<ProtocolMsg, SendError>;

  fn handle(&mut self, protocol_msg: ProtocolMsg, ctx: &mut Self::Context) -> Self::Result {
    let inner = self.inner.clone();
    ctx.spawn(async move { inner.handle_internal_msg(protocol_msg).await }.into_actor(self).map(
      move |res, _act, ctx| {
        if res.is_some() {
          ctx.binary(maxwell_protocol::encode(&res));
        }
      },
    ));
    Ok(ProtocolMsg::None)
  }
}

impl Handler {
  pub fn new(req: &HttpRequest) -> Self {
    Self { inner: Rc::new(HandlerInner::new(req)) }
  }
}

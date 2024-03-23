use std::{
  cell::{Cell, RefCell},
  hash::Hasher,
  net::{IpAddr, SocketAddr},
  rc::Rc,
  sync::atomic::{AtomicU32, Ordering},
};

use actix::{prelude::*, Actor};
use actix_web::HttpRequest;
use actix_web_actors::ws;
use ahash::{AHasher, HashMap};
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
      ProtocolMsg::RegisterServiceReq(req) => self.handle_register_service_req(req),
      ProtocolMsg::SetRoutesReq(req) => self.handle_set_routes_req(req),
      ProtocolMsg::GetRoutesReq(req) => self.handle_get_routes_req(req),
      ProtocolMsg::GetTopicDistChecksumReq(req) => self.handle_get_topic_dist_checksum_req(req),
      ProtocolMsg::GetRouteDistChecksumReq(req) => self.handle_get_route_dist_checksum_req(req),
      ProtocolMsg::PickFrontendReq(req) => self.handle_pick_frontend_req(req),
      ProtocolMsg::LocateTopicReq(req) => self.handle_locate_topic_req(req),
      ProtocolMsg::ResolveIpReq(req) => self.handle_resolve_ip_req(req),
      _ => {
        log::error!("Received unknown msg: {:?}", protocol_msg);

        maxwell_protocol::ErrorRep {
          code: ErrorCode::UnknownMsg as i32,
          desc: format!("Received unknown msg: {:?}", protocol_msg),
          r#ref: get_ref(&protocol_msg),
        }
        .into_enum()
      }
    }
  }

  async fn handle_internal_msg(self: Rc<Self>, protocol_msg: ProtocolMsg) -> ProtocolMsg {
    log::debug!("received internal msg: {:?}", protocol_msg);
    match &protocol_msg {
      _ => {
        log::error!("Received unknown msg: {:?}", protocol_msg);

        maxwell_protocol::ErrorRep {
          code: ErrorCode::UnknownMsg as i32,
          desc: format!("Received unknown msg: {:?}", protocol_msg),
          r#ref: get_ref(&protocol_msg),
        }
        .into_enum()
      }
    }
  }

  #[inline(always)]
  fn handle_ping_req(
    self: Rc<Self>, req: maxwell_protocol::PingReq,
  ) -> maxwell_protocol::ProtocolMsg {
    self.activate_node();
    maxwell_protocol::PingRep { r#ref: req.r#ref }.into_enum()
  }

  #[inline(always)]
  fn handle_register_frontend_req(
    self: Rc<Self>, req: maxwell_protocol::RegisterFrontendReq,
  ) -> maxwell_protocol::ProtocolMsg {
    let frontend_id = build_node_id(self.peer_addr.ip(), req.http_port);
    self.node_type.set(NodeType::Frontend);
    *self.node_id.borrow_mut() = Some(frontend_id.clone());

    log::info!("Registering frontend: from: {:?}, req: {:?}", frontend_id, req);

    if FRONTEND_MGR.get(&frontend_id).is_some() {
      maxwell_protocol::RegisterFrontendRep { r#ref: req.r#ref }.into_enum()
    } else {
      log::error!("Not allowed to register this frontend: {:?}", frontend_id);

      maxwell_protocol::ErrorRep {
        code: ErrorCode::NotAllowedToRegisterFrontend as i32,
        desc: format!("Not allowed to register this frontend: {:?}", frontend_id),
        r#ref: req.r#ref,
      }
      .into_enum()
    }
  }

  #[inline(always)]
  fn handle_register_backend_req(
    self: Rc<Self>, req: maxwell_protocol::RegisterBackendReq,
  ) -> maxwell_protocol::ProtocolMsg {
    self.node_type.set(NodeType::Backend);
    let backend_id = build_node_id(self.peer_addr.ip(), req.http_port);
    *self.node_id.borrow_mut() = Some(backend_id.clone());

    log::info!("Registering backend: from: {:?}, req: {:?}", backend_id, req);

    if BACKEND_MGR.get(&backend_id).is_some() {
      maxwell_protocol::RegisterBackendRep { r#ref: req.r#ref }.into_enum()
    } else {
      log::error!("Not allowed to register this backend: {:?}", backend_id);

      maxwell_protocol::ErrorRep {
        code: ErrorCode::NotAllowedToRegisterBackend as i32,
        desc: format!("Not allowed to register this backend: {:?}", backend_id),
        r#ref: req.r#ref,
      }
      .into_enum()
    }
  }

  #[inline(always)]
  fn handle_register_service_req(
    self: Rc<Self>, req: maxwell_protocol::RegisterServiceReq,
  ) -> maxwell_protocol::ProtocolMsg {
    self.node_type.set(NodeType::Service);
    let service = Service::new(self.peer_addr.ip(), req.http_port);
    *self.node_id.borrow_mut() = Some(service.id().clone());

    log::info!("Registering service: from: {:?}, req: {:?}", service.id(), req);

    SERVICE_MGR.add(service);
    maxwell_protocol::RegisterServiceRep { r#ref: req.r#ref }.into_enum()
  }

  #[inline(always)]
  fn handle_set_routes_req(
    self: Rc<Self>, req: maxwell_protocol::SetRoutesReq,
  ) -> maxwell_protocol::ProtocolMsg {
    if let Some(service_id) = self.node_id.borrow().as_ref() {
      log::info!("Setting routes: from: {:?}, req : {:?}", service_id, req);
      let pb = PathBundle {
        ws_paths: req.ws_paths.into_iter().collect(),
        get_paths: req.get_paths.into_iter().collect(),
        post_paths: req.post_paths.into_iter().collect(),
        put_paths: req.put_paths.into_iter().collect(),
        patch_paths: req.patch_paths.into_iter().collect(),
        delete_paths: req.delete_paths.into_iter().collect(),
        head_paths: req.head_paths.into_iter().collect(),
        options_paths: req.options_paths.into_iter().collect(),
        trace_paths: req.trace_paths.into_iter().collect(),
      };
      ROUTE_MGR.set_reverse_route_group(service_id.clone(), pb);
      maxwell_protocol::SetRoutesRep { r#ref: req.r#ref }.into_enum()
    } else {
      log::error!(
        "The related service has not registered: ip: {:?}, req: {:?}",
        self.peer_addr.ip(),
        req
      );

      maxwell_protocol::ErrorRep {
        code: ErrorCode::MasterError as i32,
        desc: format!(
          "The related service has not registered: ip: {:?}, req: {:?}",
          self.peer_addr.ip(),
          req
        ),
        r#ref: req.r#ref,
      }
      .into_enum()
    }
  }

  #[inline(always)]
  fn handle_get_routes_req(
    self: Rc<Self>, req: maxwell_protocol::GetRoutesReq,
  ) -> maxwell_protocol::ProtocolMsg {
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
      let is_healthy = match SERVICE_MGR.get(service_id) {
        Some(service) => service.is_healthy(),
        None => {
          log::info!("Found a stale service: id: {}", service_id);
          stale_services.push(service_id.clone());
          continue;
        }
      };
      // service_id is just the endpoint here
      let endpoint = service_id;

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

    maxwell_protocol::GetRoutesRep {
      ws_route_groups: ws_route_groups.values().cloned().collect(),
      get_route_groups: get_route_groups.values().cloned().collect(),
      post_route_groups: post_route_groups.values().cloned().collect(),
      put_route_groups: put_route_groups.values().cloned().collect(),
      patch_route_groups: patch_route_groups.values().cloned().collect(),
      delete_route_groups: delete_route_groups.values().cloned().collect(),
      head_route_groups: head_route_groups.values().cloned().collect(),
      options_route_groups: options_route_groups.values().cloned().collect(),
      trace_route_groups: trace_route_groups.values().cloned().collect(),
      r#ref: req.r#ref,
    }
    .into_enum()
  }

  fn handle_get_topic_dist_checksum_req(
    self: Rc<Self>, req: maxwell_protocol::GetTopicDistChecksumReq,
  ) -> maxwell_protocol::ProtocolMsg {
    maxwell_protocol::GetTopicDistChecksumRep { checksum: BACKEND_MGR.checksum(), r#ref: req.r#ref }
      .into_enum()
  }

  fn handle_get_route_dist_checksum_req(
    self: Rc<Self>, req: maxwell_protocol::GetRouteDistChecksumReq,
  ) -> maxwell_protocol::ProtocolMsg {
    let mut stale_services = vec![];
    let mut is_every_service_healthy = true;
    for reverse_route_group in ROUTE_MGR.reverse_route_group_iter() {
      if let Some(service) = SERVICE_MGR.get(reverse_route_group.key()) {
        if !service.is_healthy() {
          log::info!("Found an unhealthy service: id: {}", service.id());
          is_every_service_healthy = false;
          break;
        }
      } else {
        log::info!("Found a stale service: id: {}", reverse_route_group.key());
        stale_services.push(reverse_route_group.key().clone());
        is_every_service_healthy = false;
        break;
      }
    }

    for service_id in &stale_services {
      SERVICE_MGR.remove(service_id);
      ROUTE_MGR.remove_reverse_route_group(&service_id);
    }

    let seed = format!(
      "{},{}",
      ROUTE_MGR.version(),
      if is_every_service_healthy { 1 } else { Utc::now().timestamp_millis() }
    );
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(seed.as_bytes());
    let checksum = hasher.finalize();

    maxwell_protocol::GetRouteDistChecksumRep { checksum, r#ref: req.r#ref }.into_enum()
  }

  #[inline(always)]
  fn handle_pick_frontend_req(
    self: Rc<Self>, req: maxwell_protocol::PickFrontendReq,
  ) -> maxwell_protocol::ProtocolMsg {
    if let Some(frontend) = FRONTEND_MGR.pick() {
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
      maxwell_protocol::PickFrontendRep {
        endpoint: format!("{}:{}", ip, frontend.http_port),
        r#ref: req.r#ref,
      }
      .into_enum()
    } else {
      log::error!("Failed to find an available frontend.");

      maxwell_protocol::ErrorRep {
        code: ErrorCode::FailedToPickFrontend as i32,
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
        log::debug!("Found the backend: topic: {}, backend_id: {}", req.topic, backend_id);

        if let Some(backend) = BACKEND_MGR.get(&backend_id) {
          maxwell_protocol::LocateTopicRep {
            endpoint: format!("{}:{}", backend.private_ip, backend.http_port),
            r#ref: req.r#ref,
          }
          .into_enum()
        } else {
          log::error!(
            "Failed to find the backend: topic: {}, backend_id: {}",
            req.topic,
            backend_id
          );

          maxwell_protocol::ErrorRep {
            code: ErrorCode::FailedToLocateTopic as i32,
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
        if let Some(backend) = BACKEND_MGR.pick_with(|_, ids| {
          let mut hasher = AHasher::default();
          hasher.write(req.topic.as_bytes());
          let hash = hasher.finish();
          let index = hash % ids.len() as u64;
          ids.get(index as usize)
        }) {
          log::debug!("Picked the backend: topic: {}, backend_id: {}", req.topic, backend.id());

          match TOPIC_MGR.assign(req.topic.clone(), backend.id().clone()) {
            Ok(()) => maxwell_protocol::LocateTopicRep {
              endpoint: format!("{}:{}", backend.private_ip, backend.http_port),
              r#ref: req.r#ref,
            }
            .into_enum(),
            Err(err) => {
              log::error!("Failed to assign topic: {}, err: {}", req.topic, err);

              return maxwell_protocol::ErrorRep {
                code: ErrorCode::FailedToLocateTopic as i32,
                desc: format!("Failed to assign topic: {}, err: {}", req.topic, err),
                r#ref: req.r#ref,
              }
              .into_enum();
            }
          }
        } else {
          log::error!("Failed to find an available backend: topic: {}", req.topic);

          maxwell_protocol::ErrorRep {
            code: ErrorCode::FailedToLocateTopic as i32,
            desc: format!("Failed to find an available backend: topic: {}", req.topic),
            r#ref: req.r#ref,
          }
          .into_enum()
        }
      }
      Err(err) => {
        log::error!("Failed to locate topic: {}, err: {}", req.topic, err);

        maxwell_protocol::ErrorRep {
          code: ErrorCode::FailedToLocateTopic as i32,
          desc: format!("Failed to locate topic: {}, err: {}", req.topic, err),
          r#ref: req.r#ref,
        }
        .into_enum()
      }
    }
  }

  #[inline(always)]
  fn handle_resolve_ip_req(
    self: Rc<Self>, req: maxwell_protocol::ResolveIpReq,
  ) -> maxwell_protocol::ProtocolMsg {
    maxwell_protocol::ResolveIpRep { ip: self.peer_addr.ip().to_string(), r#ref: req.r#ref }
      .into_enum()
  }

  #[inline(always)]
  fn activate_node(self: Rc<Self>) {
    if let Some(node_id) = self.node_id.borrow().as_ref() {
      match self.node_type.get() {
        NodeType::Frontend => FRONTEND_MGR.activate(node_id),
        NodeType::Backend => BACKEND_MGR.activate(node_id),
        NodeType::Service => SERVICE_MGR.activate(node_id),
        _ => {}
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

pub struct Handler {
  inner: Rc<HandlerInner>,
}

impl Actor for Handler {
  type Context = ws::WebsocketContext<Self>;

  fn started(&mut self, _ctx: &mut Self::Context) {
    log::debug!("Handler actor started: id: {:?}", self.inner.id);
  }

  fn stopping(&mut self, _ctx: &mut Self::Context) -> Running {
    log::debug!("Handler actor stopping: id: {:?}", self.inner.id);
    Running::Stop
  }

  fn stopped(&mut self, _ctx: &mut Self::Context) {
    log::debug!("Handler actor stopped: id: {:?}", self.inner.id);
  }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for Handler {
  fn handle(&mut self, ws_msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
    match ws_msg {
      Ok(ws::Message::Ping(ws_msg)) => {
        self.inner.clone().activate_node();
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

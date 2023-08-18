#[macro_use]
extern crate serde_derive;

mod config;
mod db;
mod http_handler;
mod node_mgr;
mod route_mgr;
mod topic_mgr;
mod ws_handler;

use std::sync::Arc;

use actix_cors::Cors;
use actix_web::{
  http::header::ContentType, middleware, web, App, Error, HttpRequest, HttpResponse, HttpServer,
  Result,
};
use actix_web_actors::ws;

use crate::{config::CONFIG, http_handler::HttpHandler, ws_handler::Handler};

const MAX_FRAME_SIZE: usize = 134217728;

async fn ws(req: HttpRequest, stream: web::Payload) -> Result<HttpResponse, Error> {
  let rep =
    ws::WsResponseBuilder::new(Handler::new(&req), &req, stream).frame_size(MAX_FRAME_SIZE).start();
  log::info!("ws req: {:?}, rep: {:?}", req, rep);
  rep
}

async fn assign_frontend(req: HttpRequest) -> HttpResponse {
  let rep = HttpHandler::new(&req).assign_frontend();
  log::info!("http req: {:?}, rep: {:?}", req, rep);
  HttpResponse::Ok()
    .content_type(ContentType::json())
    .insert_header(("Access-Control-Allow-Origin", "*"))
    .force_close()
    .json(rep)
}

async fn get_frontends(req: HttpRequest) -> HttpResponse {
  let rep = HttpHandler::new(&req).get_frontends();
  log::info!("http req: {:?}, rep: {:?}", req, rep);
  HttpResponse::Ok()
    .content_type(ContentType::json())
    .insert_header(("Access-Control-Allow-Origin", "*"))
    .force_close()
    .json(rep)
}

#[actix_web::main]
async fn main() {
  log4rs::init_file("config/log4rs.yaml", Default::default()).unwrap();

  let store_handle = Arc::new(());

  HttpServer::new(move || {
    App::new()
      .app_data(store_handle.clone())
      .wrap(middleware::Logger::default())
      .wrap(Cors::default().allow_any_origin().allow_any_header())
      .route("/ws", web::get().to(ws))
      .route("/$assign-frontend", web::get().to(assign_frontend))
      .route("/$get-frontends", web::get().to(get_frontends))
  })
  .bind(format!("{}:{}", "0.0.0.0", CONFIG.http_port))
  .unwrap()
  .run()
  .await
  .unwrap();
}

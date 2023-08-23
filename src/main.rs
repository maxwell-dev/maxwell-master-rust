#[macro_use]
extern crate serde_derive;

mod config;
mod db;
mod handler;
mod node_mgr;
mod route_mgr;
mod topic_mgr;

use std::sync::Arc;

use actix_cors::Cors;
use actix_web::{
  http::header::ContentType, middleware, web, App, Error, HttpRequest, HttpResponse, HttpServer,
  Result,
};
use actix_web_actors::ws;

use crate::{
  config::CONFIG,
  handler::{http_handler::HttpHandler, ws_handler::Handler},
};

async fn ws(req: HttpRequest, stream: web::Payload) -> Result<HttpResponse, Error> {
  let rep = ws::WsResponseBuilder::new(Handler::new(&req), &req, stream)
    .frame_size(CONFIG.server.max_frame_size)
    .start();
  log::info!("ws req: {:?}, rep: {:?}", req, rep);
  rep
}

async fn pick_frontend(req: HttpRequest) -> HttpResponse {
  let rep = HttpResponse::Ok()
    .content_type(ContentType::json())
    .insert_header(("Access-Control-Allow-Origin", "*"))
    .force_close()
    .json(HttpHandler::new(&req).pick_frontend());
  log::info!("http req: {:?}, rep: {:?}", req, rep);
  rep
}

async fn pick_frontends(req: HttpRequest) -> HttpResponse {
  let rep = HttpResponse::Ok()
    .content_type(ContentType::json())
    .insert_header(("Access-Control-Allow-Origin", "*"))
    .force_close()
    .json(HttpHandler::new(&req).pick_frontends());
  log::info!("http req: {:?}, rep: {:?}", req, rep);
  rep
}

#[actix_web::main]
async fn main() {
  log4rs::init_file("config/log4rs.yaml", Default::default()).unwrap();

  let store_handle = Arc::new(());

  HttpServer::new(move || {
    App::new()
      .app_data(store_handle.clone())
      .wrap(middleware::Logger::default())
      .wrap(
        Cors::default()
          .allow_any_header()
          .allow_any_origin()
          .send_wildcard()
          .block_on_origin_mismatch(false)
          .expose_any_header()
          .max_age(None),
      )
      .route("/$ws", web::get().to(ws))
      .route("/$pick-frontend", web::get().to(pick_frontend))
      .route("/$pick-frontends", web::get().to(pick_frontends))
  })
  .backlog(CONFIG.server.backlog)
  .keep_alive(CONFIG.server.keep_alive)
  .max_connection_rate(CONFIG.server.max_connection_rate)
  .max_connections(CONFIG.server.max_connections)
  .workers(CONFIG.server.workers)
  .bind(format!("{}:{}", "0.0.0.0", CONFIG.server.http_port))
  .unwrap()
  .run()
  .await
  .unwrap();
}

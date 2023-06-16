#[macro_use]
extern crate serde_derive;

mod config;
mod db;
mod handler;
mod node_mgr;
mod route_mgr;

use std::sync::Arc;

use actix_web::{middleware, web, App, Error, HttpRequest, HttpResponse, HttpServer};
use actix_web_actors::ws;

use crate::{config::CONFIG, handler::Handler};

const MAX_FRAME_SIZE: usize = 134217728;

async fn index(req: HttpRequest, stream: web::Payload) -> Result<HttpResponse, Error> {
  log::info!("ws req: {:?}", req);
  let resp =
    ws::WsResponseBuilder::new(Handler::new(&req), &req, stream).frame_size(MAX_FRAME_SIZE).start();
  log::info!("ws resp: {:?}", resp);
  resp
}

#[actix_web::main]
async fn main() {
  log4rs::init_file("config/log4rs.yaml", Default::default()).unwrap();

  let store_handle = Arc::new(());

  HttpServer::new(move || {
    App::new()
      .app_data(store_handle.clone())
      .wrap(middleware::Logger::default())
      .route("/ws", web::get().to(index))
  })
  .bind(format!("{}:{}", "0.0.0.0", CONFIG.http_port))
  .unwrap()
  .run()
  .await
  .unwrap();
}

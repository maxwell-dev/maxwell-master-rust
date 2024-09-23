#[macro_use]
extern crate serde_derive;

mod config;
mod db;
mod handler;
mod node_mgr;
mod route_mgr;
mod topic_mgr;

use std::{fs::File, io::BufReader};

use actix_cors::Cors;
use actix_web::{
  http::header::ContentType, middleware, web, App, Error, HttpRequest, HttpResponse, HttpServer,
};
use actix_web_actors::ws;
use anyhow::{anyhow, Result};
use futures::future;
use rustls::ServerConfig;
use rustls_pemfile::{certs, private_key};

use crate::{
  config::CONFIG,
  handler::{http_handler::HttpHandler, ws_handler::Handler},
};

static SERVER_NAME: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"),);

async fn health(_req: HttpRequest) -> Result<HttpResponse, Error> {
  Ok(HttpResponse::Ok().body(""))
}

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
    .force_close()
    .json(HttpHandler::new(&req).pick_frontend());
  log::info!("http req: {:?}, rep: {:?}", req, rep);
  rep
}

async fn pick_frontends(req: HttpRequest) -> HttpResponse {
  let rep = HttpResponse::Ok()
    .content_type(ContentType::json())
    .force_close()
    .json(HttpHandler::new(&req).pick_frontends());
  log::info!("http req: {:?}, rep: {:?}", req, rep);
  rep
}

#[actix_web::main]
async fn main() -> Result<()> {
  log4rs::init_file("config/log4rs.yaml", Default::default())?;
  future::try_join(create_http_server(false), create_http_server(true)).await?;
  Ok(())
}

async fn create_http_server(is_https: bool) -> Result<()> {
  let http_server = HttpServer::new(move || {
    App::new()
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
      .wrap(
        middleware::DefaultHeaders::new()
          .add(("Access-Control-Allow-Origin", "*"))
          .add(("Server", SERVER_NAME)),
      )
      .route("/$health", web::get().to(health))
      .route("/$ws", web::get().to(ws))
      .route("/$pick-frontend", web::get().to(pick_frontend))
      .route("/$pick-frontends", web::get().to(pick_frontends))
  })
  .backlog(CONFIG.server.backlog)
  .keep_alive(CONFIG.server.keep_alive)
  .max_connection_rate(CONFIG.server.max_connection_rate)
  .max_connections(CONFIG.server.max_connections)
  .workers(CONFIG.server.workers);

  if is_https {
    http_server.bind_rustls_0_23(
      format!("{}:{}", "0.0.0.0", CONFIG.server.https_port),
      create_tls_config()?,
    )?
  } else {
    http_server.bind(format!("{}:{}", "0.0.0.0", CONFIG.server.http_port))?
  }
  .run()
  .await
  .map_err(|err| anyhow!("Failed to run the server: err: {:?}", err))
}

fn create_tls_config() -> Result<ServerConfig> {
  let cert_file = File::open(CONFIG.server.cert_file.clone())?;
  let key_file = File::open(CONFIG.server.key_file.clone())?;

  let cert_buf = &mut BufReader::new(cert_file);
  let key_buf = &mut BufReader::new(key_file);

  let cert_chain = certs(cert_buf).collect::<Result<Vec<_>, _>>()?;
  let key = private_key(key_buf)?.ok_or(anyhow!("no key found"))?;

  Ok(ServerConfig::builder().with_no_client_auth().with_single_cert(cert_chain, key)?)
}

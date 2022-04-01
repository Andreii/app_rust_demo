use actix_web::{get, middleware, post, web, App, HttpResponse, HttpServer, Responder, Result};
use serde::Serialize;
use scylla::macros::FromRow;
use scylla::transport::session::{IntoTypedRows, Session};
use scylla::SessionBuilder;
use std::env;
use std::sync::Arc;
extern crate rand;

#[derive(Serialize)]
struct MyObj {
    name: String,
    test: String,
    author: String,
    cost: i32,
    unit: String,
    target: String,
}

#[get("/a/{name}")]
async fn hello(
    name: web::Path<String>,
    data: web::Data<AppState>,
) -> Result<impl Responder> {
    let obj = MyObj {
        name: name.to_string(),
        test: "this is even a longerer string".to_string(),
        author: "Jules Verne".to_string(),
        cost: 30,
        unit: "EUR".to_string(),
        target: "sale".to_string(),
    };


    let prepared = Arc::new(
        data.scy_session
            .prepare("INSERT INTO ks.t (a, b, c) VALUES (?, 7, ?)")
            .await
            .unwrap()
    );
    let rnd : i32 = rand::random();

    data.scy_session
        .execute(&prepared, (rnd, "I'm prepared!"))
        .await
        .unwrap();

    Ok(web::Json(obj))
}

struct AppState {
    scy_session: Session,
    app_name: String,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let session: Session = SessionBuilder::new()
        .known_node(env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string()))
        .user("username", "passwor")
        .build()
        .await
        .expect("Some error message");
    let data = web::Data::new(AppState {
        scy_session: session,
        app_name: String::from("Actix-web"),
    });

    data.scy_session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await.unwrap();

    data.scy_session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.t (a int, b int, c text, primary key (a, b))",
            &[],
        )
        .await
        .unwrap();

    HttpServer::new(move|| {
        App::new()
            .app_data(data.clone())
            .wrap(middleware::Compress::default())
            .service(hello)
    })
        .bind(("0.0.0.0", 6006))?
        .run()
        .await
}

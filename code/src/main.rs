use lsm_db::db::db::{Config, Db};
use lsm_db::server::server::Server;

fn main() {
    // create a db with the default config.
    let db = Db::new(Config::default());
    // create a server on which runs the db.
    Server::new(db).run();
}

use lsm_db::config::config::Config;
use lsm_db::db::db::Db;
use lsm_db::server::server::Server;

fn main() {
    // create a db with the default config.
    let db = Db::new(Config::default());
    // create a server on which runs the db.
    Server::new(db).run();
}

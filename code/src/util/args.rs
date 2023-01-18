use clap::Parser;

/// command line options.
/// deprecated for now.
#[derive(Parser)]
pub struct Args {
    /// host address.
    #[arg(short = 'h', long = "host", default_value_t = String::from("127.0.0.1"))]
    pub host: String,
    /// port number.
    #[arg(short = 'p', long = "port")]
    pub port: i32,
}

use clap::{command, Parser};

#[derive(Debug, clap::Subcommand)]
pub enum Action {
    // validate integrity of the db with respect to the sql files
    Check,
    /// removes metadata table which tacks migrations
    Clean,
    /// sets up database to track migrations
    Init,
    // perform database migration using all .sql files in directory
    Migrate,
}

impl std::fmt::Display for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[command(subcommand)]
    pub action: Action,

    #[arg(short, long)]
    pub username: Option<String>,

    #[arg(long)]
    pub hostname: Option<String>,

    #[arg(short, long)]
    pub port: Option<usize>,
}

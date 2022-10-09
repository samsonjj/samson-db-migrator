use clap::{command, Parser};
use postgres::{self, Client, NoTls};

mod migrator;
mod util;
mod sql;

use migrator::{Metadata, Migrator};

fn map_pg_err(e: postgres::Error) -> String {
    if let Some(db_error) = e.as_db_error() {
        format!(
            "DbError[{}|{}]: {}",
            db_error.code().code(),
            db_error.severity(),
            db_error.message()
        )
    } else {
        e.to_string()
    }
}

fn main() -> Result<(), String> {
    // let args = ComputedArgs::from_args(Args::parse());
    let args = Args::parse();
    

    match args.action {
        Action::Init => {
            let mut client = sql::get_pg_client().map_err(map_pg_err)?;
            Metadata::init_metadata_table(&mut client).map_err(map_pg_err)?;
        },
        Action::Clean => {
            let mut client = sql::get_pg_client().map_err(map_pg_err)?;
            println!("Are you sure you want to run clean? This will remove the metadata table from the database, removing all record of past migrations. If you are sure, type 'pee pee poo poo'");

            let query = "
                DROP TABLE IF EXISTS samson_db_migrator_metadata;
            ";
            client.execute(query, &[]).map_err(map_pg_err)?;
        },
        Action::Check => {
            let mut client = sql::get_pg_client().map_err(map_pg_err)?;
            let metadata = Metadata::from_postgres_client(&mut client).map_err(map_pg_err)?;
            migrator::MetadataRow::print_headers();
            for row in metadata.rows {
                row.print_row();
            }
        },
        Action::Migrate => {
            let mut client = sql::get_pg_client().map_err(map_pg_err)?;
            let metadata = Metadata::from_postgres_client(&mut client);
            Migrator::new().unwrap().do_migration(&mut client).unwrap();
        },
    }

    println!("your action was {}", args.action);
    Ok(())
}

#[derive(Debug, clap::Subcommand)]
pub enum Action {
    // validate integrity of the db with respect to the sql files
    Check,
    /// removes metadata table which tacks migrations
    Clean,
    /// sets up database to track migrations
    Init,
    // perform database migration using all .sql files in directory
    Migrate
}

impl std::fmt::Display for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    action: Action,

    #[arg(short, long)]
    username: Option<String>,

    #[arg(long)]
    hostname: Option<String>,

    #[arg(short, long)]
    port: Option<usize>,
}

struct ComputedArgs {
    action: Action,
    username: String,
    hostname: String,
    port: usize,
}

impl ComputedArgs {
    pub fn from_args(args: Args) -> Self {
        Self {
            action: args.action,
            username: args.username.unwrap_or("postgres".to_string()),
            hostname: args.hostname.unwrap_or("localhost".to_string()),
            port: args.port.unwrap_or(5432)
        }
    }
}
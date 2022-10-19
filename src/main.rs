mod cli;
mod filesystem;
mod migrator;
mod sql;
mod util;

use clap::Parser;
use cli::{Action, Args};

use migrator::{Metadata, Migrator};
use sql::MetadataRow;

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
    let _args = Args::parse();
    let args = ComputedArgs::from_args(Args::parse());

    match args.action {
        Action::Init => {
            let mut client = sql::get_pg_client().map_err(map_pg_err)?;
            sql::init_metadata_table(&mut client).map_err(map_pg_err)?;
        }
        Action::Clean => {
            let mut client = sql::get_pg_client().map_err(map_pg_err)?;
            println!("Are you sure you want to run clean? This will remove the metadata table from the database, removing all record of past migrations. If you are sure, type 'pee pee poo poo'");

            let query = "
                DROP TABLE IF EXISTS samson_db_migrator_metadata;
            ";
            client.execute(query, &[]).map_err(map_pg_err)?;
        }
        Action::Check => {
            let mut client = sql::get_pg_client().map_err(map_pg_err)?;
            let metadata_rows =
                MetadataRow::get_rows(&mut client).map_err(map_pg_err)?;
            MetadataRow::print_headers();
            for row in metadata_rows {
                row.print_row();
            }
        }
        Action::Migrate => {
            let mut client = sql::get_pg_client().map_err(map_pg_err)?;
            Migrator::new().unwrap().do_migration(&mut client).unwrap();
        }
    }

    println!("your action was {}", args.action);
    Ok(())
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
            port: args.port.unwrap_or(5432),
        }
    }
}

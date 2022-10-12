use postgres;

use crate::util;

pub fn get_pg_client() -> Result<postgres::Client, postgres::Error> {
    let user = "postgres";
    let hostname = "localhost";
    let database = "postgres";
    let password = util::ask_password();

    let connection_string = get_connection_string(user, &password, hostname, database);
    let client = postgres::Client::connect(&connection_string, postgres::NoTls)?;
    println!("{}", format!("connected to {}...", connection_string));
    Ok(client)
}

fn get_connection_string(user: &str, password: &str, hostname: &str, database: &str) -> String {
    format!(
        "postgresql://{}:{}@{}/{}",
        user, password, hostname, database
    )
}

use std::cell::RefCell;

#[derive(Clone, Debug)]
pub struct MetadataRow {
    checksum: i32,
    filename: String,
    ts: chrono::DateTime<chrono::Utc>,
}

struct MetadataRepository {
    client: RefCell<postgres::Client>,
}

impl MetadataRepository {
    pub fn new(client: RefCell<postgres::Client>) -> Self {
        Self { client }
    }

    fn insert_metadata_row(
        tx: &mut postgres::Transaction,
        data: MetadataRow,
    ) -> Result<(), postgres::Error> {
        let query = "
            INSERT INTO samson_db_migrator_metadata (ts, filename, checksum) VALUES($1, $2, $3);
        ";
        tx.execute(
            query,
            &[
                &chrono::Utc::now(),
                &&data.filename,
                &(data.checksum as i64),
            ],
        )?;
        Ok(())
    }
}

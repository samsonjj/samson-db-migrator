use std::cell::RefCell;

use chrono::{DateTime, Utc};
use postgres::GenericClient;

use crate::{filesystem::FileData, migrator::Metadata, util};
use colored::*;

macro_rules! row_format {
    () => {
        "{0: <30} | {1: <30} | {2: <10}"
    };
}

pub fn init_metadata_table(
    client: &mut postgres::Client,
) -> Result<(), postgres::Error> {
    let query = "
            CREATE TABLE samson_db_migrator_metadata (
                ts                  TIMESTAMP WITH TIME ZONE,
                filename            TEXT,
                checksum            BIGINT
            );
        ";
    client.execute(query, &[])?;
    println!(
        "{}",
        "initialized samson_db_migrator_metadata table".green()
    );
    Ok(())
}

pub fn remove_metadata_table(
    client: &mut postgres::Client,
) -> Result<(), Box<dyn std::error::Error>> {
    let query = "
            DROP TABLE IF EXISTS samson_db_migrator_metadata
        ";
    client.execute(query, &[])?;
    println!("{}", "removed samson_db_migrator_metadata table".red());
    Ok(())
}

pub fn get_pg_client() -> Result<postgres::Client, postgres::Error> {
    let user = "postgres";
    let hostname = "localhost";
    let database = "postgres";
    let password = util::ask_password();

    let connection_string =
        get_connection_string(user, &password, hostname, database);
    let client =
        postgres::Client::connect(&connection_string, postgres::NoTls)?;
    println!("{}", format!("connected to {}...", connection_string));
    Ok(client)
}

fn get_connection_string(
    user: &str,
    password: &str,
    hostname: &str,
    database: &str,
) -> String {
    format!(
        "postgresql://{}:{}@{}/{}",
        user, password, hostname, database
    )
}

#[derive(Clone, Debug)]
pub struct MetadataRow {
    pub checksum: i32,
    pub filename: String,
    pub ts: chrono::DateTime<chrono::Utc>,
}

impl MetadataRow {
    pub fn new(checksum: i32, filename: String, ts: DateTime<Utc>) -> Self {
        Self {
            checksum,
            filename,
            ts,
        }
    }

    pub fn print_headers() {
        println!();
        println!(row_format!(), "ts", "filename", "checksum",);
        println!("------------------------------------------------------------------------------------------");
    }

    pub fn print_row(&self) {
        println!(row_format!(), self.ts, self.filename, self.checksum);
    }

    pub fn insert(
        &self,
        client: &mut impl GenericClient,
    ) -> Result<(), postgres::Error> {
        let query = "
            INSERT INTO samson_db_migrator_metadata
                (ts, filename, checksum)
            VALUES($1, $2, $3);
        ";

        client.execute(
            query,
            &[
                &chrono::Utc::now(),
                &&self.filename,
                &(self.checksum as i64),
            ],
        )?;

        Ok(())
    }

    pub fn get_rows(
        client: &mut impl GenericClient,
    ) -> Result<Vec<Self>, postgres::Error> {
        let query = "
            SELECT ts, filename, checksum
            FROM samson_db_migrator_metadata
            ORDER BY ts
        ";

        let rows = client.query(query, &[])?;

        let rows = rows
            .iter()
            .map(|row| MetadataRow {
                ts: rows[0].get::<_, DateTime<Utc>>(0),
                filename: row.get(1),
                checksum: row.get::<_, i64>(2) as i32,
            })
            .collect();

        Ok(rows)
    }
}

impl From<&FileData> for MetadataRow {
    fn from(data: &FileData) -> Self {
        Self {
            checksum: data.checksum.clone(),
            filename: data.filename.clone(),
            ts: data.ts,
        }
    }
}

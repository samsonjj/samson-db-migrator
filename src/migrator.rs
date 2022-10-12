use chrono::{Date, DateTime, Utc};
use itertools::Itertools;
use std::collections::{hash_map::Entry, HashMap};
use std::env;
use std::fmt::Display;
use std::fs::{self, FileType};
use std::io::{self, Write};
use std::path::PathBuf;

use colored::Colorize;

use postgres::{self, Client};

use crate::sql;

use crate::util;

#[derive(Clone, Debug)]
pub struct MetadataRow {
    checksum: i32,
    filename: String,
    ts: chrono::DateTime<chrono::Utc>,
}

macro_rules! row_format {
    () => {
        "{0: <30} | {1: <30} | {2: <10}"
    };
}

impl MetadataRow {
    pub fn print_headers() {
        println!();
        println!(row_format!(), "ts", "filename", "checksum",);
        println!("------------------------------------------------------------------------------------------");
    }
    pub fn print_row(&self) {
        println!(row_format!(), self.ts, self.filename, self.checksum);
    }
}

#[derive(Clone, Debug)]
pub struct FileData {
    checksum: i32,
    filename: String,
    path: PathBuf,
    ts: chrono::DateTime<chrono::Utc>,
}

impl FileData {
    pub fn get_map(paths: &Vec<PathBuf>) -> HashMap<String, Self> {
        paths
            .iter()
            .map(|path| {
                (
                    path.file_name().unwrap().to_str().unwrap().to_owned(),
                    FileData {
                        checksum: 0,
                        filename: path.file_name().unwrap().to_str().unwrap().to_owned(),
                        path: path.clone(),
                        ts: Utc::now(),
                    },
                )
            })
            .collect()
    }
}

#[derive(Clone, Debug)]
pub struct Metadata {
    pub rows: Vec<MetadataRow>,
}

impl Metadata {
    pub fn from_postgres_client(client: &mut postgres::Client) -> Result<Self, postgres::Error> {
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
        Ok(Self { rows })
    }

    pub fn as_map(&self) -> HashMap<String, MetadataRow> {
        self.rows
            .iter()
            .map(|row| (row.filename.clone(), row.clone()))
            .collect()
    }

    pub fn init_metadata_table(client: &mut postgres::Client) -> Result<(), postgres::Error> {
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
}

/// Entrypoint for samson-db-migrator library, excluding cli
pub struct Migrator {
    directory: PathBuf,
}

enum MigrationStatus {
    Migrated,
    Unmigrated,
    FileMissing,
}

struct MetadataComparison {
    filename: String,
    table_row: Option<MetadataRow>,
    filesystem: Option<FileData>,
    status: Option<MigrationStatus>,
}

impl MetadataComparison {
    fn new(filename: String) -> Self {
        Self {
            filename,
            table_row: None,
            filesystem: None,
            status: None,
        }
    }
}

fn gen_comparisons(
    filedata: HashMap<String, FileData>,
    metadata: HashMap<String, MetadataRow>,
) -> Vec<MetadataComparison> {
    let mut hm: HashMap<String, MetadataComparison> = HashMap::new();
    for (filename, data) in filedata {
        let entry = hm
            .entry(filename.clone())
            .or_insert(MetadataComparison::new(filename.clone()));
        (*entry).filesystem = Some(data);
    }
    for (filename, data) in metadata {
        let entry = hm
            .entry(filename.clone())
            .or_insert(MetadataComparison::new(filename.clone()));
        (*entry).table_row = Some(data);
    }

    let keys = hm
        .keys()
        .sorted()
        .map(|k| k.clone())
        .collect::<Vec<String>>();
    keys.iter()
        .map(|k| hm.remove(k).unwrap())
        .collect::<Vec<_>>()
}

impl Migrator {
    pub fn new() -> Result<Self, io::Error> {
        Ok(Self {
            directory: env::current_dir()?,
        })
    }

    pub fn do_checks(
        &self,
        client: &mut postgres::Client,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let files = self.get_sorted_target_files()?;

        let metadata = Metadata::from_postgres_client(client)?;

        let filedata = FileData::get_map(&files);

        // already sorted
        let comparisons = gen_comparisons(filedata, metadata.as_map());

        let mut healthy = true;
        let mut finished_migrations = false;
        for comparison in comparisons {
            if let None = comparison.filesystem {
                println!(
                    "{}",
                    format!(
                        "no matching file found for record '{}'",
                        comparison.table_row.unwrap().filename.yellow()
                    )
                    .red()
                );
                healthy = false;
                return Ok(());
            }
            if let None = comparison.table_row {
                finished_migrations = true;
            }
        }

        Ok(())
    }

    pub fn do_migration(
        &self,
        client: &mut postgres::Client,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let files = self.get_sorted_target_files()?;
        println!("Found target files:");
        util::print_iter(Box::new(
            files
                .iter()
                .map(|x| x.file_name().unwrap().to_str().unwrap().green()),
        ));

        let metadata = Metadata::from_postgres_client(client)?;

        let filedata = FileData::get_map(&files);

        // already sorted
        let comparisons = gen_comparisons(filedata, metadata.as_map());

        for comparison in comparisons {
            if let None = comparison.filesystem {
                eprintln!(
                    "{}",
                    format!(
                        "no matching file found for record '{}'",
                        comparison.table_row.unwrap().filename.yellow()
                    )
                    .red()
                );
                return (Err(Box::new(std::io::Error::new(
                    io::ErrorKind::InvalidData,
                    "^",
                ))));
            }
            if let None = comparison.table_row {
                // perform migration
                Self::migrate_single_file(client, &comparison.filesystem.unwrap().path, &metadata)?;
            }
        }
        Ok(())
    }

    fn migrate_single_file(
        client: &mut postgres::Client,
        path: &PathBuf,
        metadata: &Metadata,
    ) -> Result<(), Box<dyn std::error::Error>> {
        dbg!();
        let mut tx = client.transaction()?;
        dbg!();
        Self::execute_sql_script(&mut tx, path)?;
        let data = MetadataRow {
            checksum: 0,
            filename: path.file_name().unwrap().to_owned().into_string().unwrap(),
            ts: chrono::Utc::now(),
        };
        dbg!();
        Self::insert_metadata_row(&mut tx, data)?;
        dbg!();
        tx.commit()?;
        dbg!();
        println!(
            "{}",
            format!("successfully migrated {:?}", path.file_name().unwrap()).green()
        );
        Ok(())
    }

    fn execute_sql_script(
        tx: &mut postgres::Transaction,
        path: &PathBuf,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let query = fs::read_to_string(path)?;
        tx.execute(&query, &[])?;
        Ok(())
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

    /// Get a list of the executable sql scripts. This includes any sql
    /// files in the current directory.
    pub fn get_sorted_target_files(&self) -> Result<Vec<PathBuf>, io::Error> {
        let mut result = fs::read_dir(&self.directory)?
            .into_iter()
            .filter_map(|x| x.ok())
            .filter(|x| FileType::is_file(&x.file_type().unwrap()))
            .filter(|x| (x.file_name().to_str()).unwrap().ends_with(".sql"))
            .map(|x| x.path())
            .collect::<Vec<_>>();

        result.sort();

        Ok(result)
    }
}

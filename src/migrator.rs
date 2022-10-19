use chrono::{DateTime, Utc};
use itertools::Itertools;
use std::collections::HashMap;
use std::env;
use std::fs::{self, FileType};
use std::io;
use std::path::PathBuf;

use colored::Colorize;

use crate::filesystem::FileData;
use crate::sql::MetadataRow;
use crate::util;

#[derive(Clone, Debug)]
pub struct Metadata {
    pub rows: Vec<MetadataRow>,
}

pub fn as_map(rows: Vec<MetadataRow>) -> HashMap<String, MetadataRow> {
    rows.iter()
        .map(|row| (row.filename.clone(), row.clone()))
        .collect()
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
    file_data: Option<FileData>,
    status: MigrationStatus,
}

impl MetadataComparison {
    fn new(filename: String) -> Self {
        Self {
            filename,
            table_row: None,
            file_data: None,
            status: MigrationStatus::Unmigrated,
        }
    }
}

fn gen_comparisons(
    filedata: HashMap<String, FileData>,
    metadata: HashMap<String, MetadataRow>,
) -> Vec<MetadataComparison> {
    // collect file and database data
    let mut hm: HashMap<String, MetadataComparison> = HashMap::new();
    for (filename, data) in filedata {
        let entry = hm
            .entry(filename.clone())
            .or_insert(MetadataComparison::new(filename.clone()));
        (*entry).file_data = Some(data);
    }
    for (filename, data) in metadata {
        let entry = hm
            .entry(filename.clone())
            .or_insert(MetadataComparison::new(filename.clone()));
        (*entry).table_row = Some(data);
    }

    // store as sorted vec
    let keys = hm
        .keys()
        .sorted()
        .map(|k| k.clone())
        .collect::<Vec<String>>();
    let mut results = keys
        .iter()
        .map(|k| hm.remove(k).unwrap())
        .collect::<Vec<_>>();

    // compute status
    for mut data in results.iter_mut() {
        data.status = match (&data.file_data, &data.table_row) {
            (Some(_), Some(_)) => MigrationStatus::Migrated,
            (None, Some(_)) => MigrationStatus::FileMissing,
            (Some(_), None) => MigrationStatus::Unmigrated,
            _ => panic!("invalid comparison between files and database"),
        }
    }

    results
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

        let metadata_rows = MetadataRow::get_rows(client)?;

        let filedata = FileData::get_map(&files);

        // already sorted
        let comparisons = gen_comparisons(filedata, as_map(metadata_rows));

        let mut healthy = true;
        let mut finished_migrations = false;
        for comparison in comparisons {
            if let None = comparison.file_data {
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

        let metadata_rows = MetadataRow::get_rows(client)?;

        let filedata = FileData::get_map(&files);

        // already sorted
        let comparisons = gen_comparisons(filedata, as_map(metadata_rows));

        for comparison in comparisons {
            match comparison.status {
                MigrationStatus::FileMissing => {
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
                MigrationStatus::Unmigrated => {
                    // perform migration
                    Self::migrate_single_file(
                        client,
                        &comparison.file_data.as_ref().unwrap(),
                    )?;
                }
                MigrationStatus::Migrated => {
                    println!(
                        "already migrated {:?}",
                        comparison.file_data.unwrap().filename
                    );
                }
            }
        }
        Ok(())
    }

    fn migrate_single_file(
        client: &mut postgres::Client,
        file_data: &FileData,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // execute sql file
        let mut tx = client.transaction()?;
        Self::execute_sql_script(&mut tx, &file_data.path)?;

        // insert sql row
        let data = MetadataRow {
            checksum: 0,
            filename: file_data
                .path
                .file_name()
                .unwrap()
                .to_owned()
                .into_string()
                .unwrap(),
            ts: chrono::Utc::now(),
        };

        MetadataRow::from(file_data).insert(&mut tx);

        // commit
        tx.commit()?;

        println!(
            "{}",
            format!(
                "successfully migrated {:?}",
                file_data.path.file_name().unwrap()
            )
            .green()
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

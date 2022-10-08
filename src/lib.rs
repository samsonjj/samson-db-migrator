use std::env;
use std::fs::{self, FileType};
use std::io::{self, Write};
use std::path::PathBuf;

use postgres;

mod util;

/// Entrypoint for samson-db-migrator library, excluding cli
pub struct Migrator {
    directory: PathBuf,
}

impl Migrator {
    pub fn new() -> Result<Self, io::Error> {
        Ok(Self {
            directory: env::current_dir()?,
        })
    }

    pub fn do_migration(&self) -> Result<(), Box<dyn std::error::Error>> {
        let files = self.get_target_files()?;
        println!("Found target files:");
        util::print_vec(Box::new(
            files
                .iter()
                .map(|x| x.file_name().unwrap().to_str().unwrap()),
        ));
        for path in files {
            Self::migrate_single_file(path);
        }
        Ok(())
    }

    fn migrate_single_file(path: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
        println!("migrating {:?}", path);
        Ok(())
    }

    fn get_pg_client() -> Result<postgres::Client, postgres::Error> {
        let user = "postgres";
        let hostname = "localhost";
        let database = "postgres";
        let password = Self::ask_password();

        let connection_string = Self::get_connection_string(user, &password, hostname, database);
        postgres::Client::connect(&connection_string, postgres::NoTls)
    }

    fn get_connection_string(user: &str, password: &str, hostname: &str, database: &str) -> String {
        format!(
            "postgresql://{}:{}@{}/{}",
            user, password, hostname, database
        )
    }

    fn ask_password() -> String {
        use rpassword;
        std::io::stdout().flush().unwrap();
        rpassword::read_password().unwrap()
    }

    /// Get a list of the executable sql scripts. This includes any sql
    /// files in the current directory.
    pub fn get_target_files(&self) -> Result<Vec<PathBuf>, io::Error> {
        let result = fs::read_dir(&self.directory)?
            .into_iter()
            .filter_map(|x| x.ok())
            .filter(|x| FileType::is_file(&x.file_type().unwrap()))
            .filter(|x| (x.file_name().to_str()).unwrap().ends_with(".sql"))
            .map(|x| x.path())
            .collect::<Vec<_>>();

        Ok(result)
    }
}

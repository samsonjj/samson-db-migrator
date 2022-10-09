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

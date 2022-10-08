use postgres::{self, Client, NoTls};

use samson_db_migrator::Migrator;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let migrator = Migrator::new()?;
    migrator.do_migration()?;

    Ok(())
}

fn something() -> Result<(), postgres::Error> {
    let user = "postgres";
    let password = "xxxxxxx";
    let hostname = "localhost";
    let database = "postgres";

    let connection_string = format!(
        "postgresql://{}:{}@{}/{}",
        user, password, hostname, database
    );

    let mut client = Client::connect(&connection_string, NoTls)?;

    // let transaction = client.transaction()?;
    // transaction.commit();

    // client.commit

    client.batch_execute(
        "
        CREATE TABLE IF NOT EXISTS author (
            id              SERIAL PRIMARY KEY,
            name            VARCHAR NOT NULL,
            country         VARCHAR NOT NULL
        )
    ",
    )?;

    Ok(())
}

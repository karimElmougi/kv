use std::path::Path;

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    db_name: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Set { key: String, value: String },
    Unset { key: String },
    Get { key: String },
}

fn main() -> Result<(), kv::Error> {
    let cli = Cli::parse();

    let store = kv::Store::<String>::open(Path::new(&format!("{}.db", cli.db_name))).unwrap();

    match cli.command {
        Command::Set { key, value } => store.set(&key, &value)?,
        Command::Unset { key } => store.unset(&key)?,
        Command::Get { key } => {
            let value = store.get(&key)?;
            println!("{}", value.unwrap_or_default());
        }
    }

    Ok(())
}

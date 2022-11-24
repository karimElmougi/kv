use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    db_path: PathBuf,

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

    let store = kv::Store::<serde_json::Value>::open(&cli.db_path).unwrap();

    match cli.command {
        Command::Set { key, value } => {
            let value =
                serde_json::from_str(&value).map_err(|err| kv::Error::Write(err.to_string()))?;
            store.set(&key, &value)?
        }
        Command::Unset { key } => store.unset(&key)?,
        Command::Get { key } => {
            let value = store.get(&key)?;
            println!("{}", value.unwrap_or_default());
        }
    }

    Ok(())
}

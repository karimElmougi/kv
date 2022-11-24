# kv

A dead-simple key-value store.

## Data Format

Key-value pairs are simply appended to a file in a CSV scheme, with the value being a JSON serialization of whatever datatype is being stored.
This means that any tooling that works on CSV files can be used to inspect or modify the database transparently.
Indeed, while `kv` provides a CLI tool for handling the data, one can query the database with just base shell commands like so:
```sh
grep '^some_key,' | tail -n 1 | sd '^.+,(.+)' '$1' | jq
```

Since data is only ever appended without checking that a key already exists, the entire database must be scanned to find the latest entry for a key.
This is not ideal, but is sufficiently fast on modern drives for use in small projects.

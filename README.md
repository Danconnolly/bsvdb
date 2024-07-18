# bsvdb
Various tools &amp; libraries for BitcoinSV, written in Rust

Work in progress.

## Configuration

A configuration file is needed to run most commands. The configuration file is a TOML file called `bsvdb.toml` that is 
currently expected to be in the directory from which the `bsvdb-cli` command is run. The location of the configuration file can be
specified as a parameter to the `bsvdb-cli` command.

A full configuration file called `bsvdb_full.toml` lists all possible configuration parameters and is located in the project root directory.

Each component has its own configuration section.

You must specify the configuration for components that you use and which do not have defaults. If you do not use the component, then
you do not need to specify its configuration. If the component has defaults for all configuration values, and these defaults are
acceptable to you, then you do not need to specify its configuration.

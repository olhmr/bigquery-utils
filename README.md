# BigQuery Utility Functions

This is a collection of various utility functions that make life easier when working with BigQuery.

They're all driven by use cases that I've encountered, for example in archiving tables.

# Commands

Functionality is split into separate commands, each of which may have subcommands.

There are two global flags that apply to all commands: `--debug` (or `-d`) and `--verbose` (or `-v`). These set the logging level to `DEBUG` or `INFO`, respectively. Default log level is `WARNING`.

## archive

The archive command requires at a minimum a `target` subcommand. It will do three things:

1. Create an archive dataset. This is set to `archive` in the same project as the `target` unless `destination` is specified.
2. Copy the `target` to the same name in the `archive` dataset.
3. Delete the `target`

If copying fails, the `target` is not deleted. Additionally, input is required to confirm deletion before it happens.

# To-do
- add function for iterating through a list of tables in a dataset
- add better exception handling
- add more tests
- create options for handling situations where archive already exists
  - shard the archive using date suffix
  - replace the existing archive
  - skip copy and just delete

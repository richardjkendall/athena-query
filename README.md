# athena-query

Command line utility for querying AWS Athena, seeks to emulate sqlite3.  It implements some similar commands (the . notation) as sqlite3.

It provides an interactive shell where you can run SQL queries against AWS Athena.  The utility sends the query to Athena and then displays the results as a table or as CSV.

## How to use

1. Download a binary for your platform from the releases page
2. Set up your AWS credentials in the normal way (for example in your $HOME/.aws/ directory or via instance meta-data)
3. Put the athena-query binary in your $PATH
4. Run `athena-query --work-group <name of work group> --database <name of database>`

You can type SQL queries at the prompt.  SQL queries end with a semi-colon ';'.  You can split SQL queries across multiple lines, just use the enter key.

Special commands begin with a full-stop '.'.  Type `.help` to get a list of those available commands.

## Outputs

By default the tool outputs pretty-printed tables to STDOUT.  You can change this to CSV with the `.mode` command and you can redirect this to a file with the `.output` command.

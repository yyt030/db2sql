NAME:
   main.exe - A new cli application

USAGE:
   main.exe [global options] command [command options] [arguments...]

VERSION:
   0.0.0

COMMANDS:
     help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --dsn value, -d value     the DSN to connect, e.g.: user:passwd@ip:port/dbname
   --table value, -t value   table name to operate, e.g.: schema.table
   --conc value, -c value    the number of concurrent processor (default: 1)
   --number value, -n value  the number of all execute sql (default: 10)
   --max value, -m value     the max number of one transaction (default: 5)
   --sql value, -s value     the type of sql. e.g.: 1->insert, 2->update, 4->delete, 3->insert+update, 7->insert/update/delte (default: 1)
   --rate value, -r value    the rate of rollback. e.g.: 0.5-> half of rollback (default: 0)
   --help, -h                show help
   --version, -v             print the version

error: pls input dsn or table

#!/usr/bin/env bash
export PGPASSWORD=admin
export PGHOST=localhost
export PGDATABASE=un
export PGUSER=admin

when=$(date "+%c-%m-%d" | awk '{printf $5}')
output="./db-export-$when.tar"

echo "Connecting to the database..."

pg_dump -F t -C > $output 
gzip $output

echo "Exported the database to disk:  $output.gz" 

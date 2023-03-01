#!/bin/bash
# Bash scripts to install postgresql server in ubuntu image.
# It also creates "root" user with all priviledges on "postgres" database for 
# testing purpose. The password is "testpassword".
apt update
apt install postgresql postgresql-contrib -y
service postgresql start
su postgres
psql -c '\x' -c "CREATE USER root WITH PASSWORD 'testpassword';"
psql -c '\x' -c "GRANT ALL PRIVILEGES ON DATABASE postgres TO root;"
exit

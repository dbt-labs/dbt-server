#!/bin/bash
# Bash scripts to install postgresql server in ubuntu image.
# It also creates "root" user with all priviledges on "postgres" database for 
# testing purpose. The password is "testpassword".
sudo apt update
sudo apt install postgresql postgresql-contrib -y
sudo service postgresql start
TEMP_SQL_FILE=/tmp/__temp.sql
cat <<EOF  >${TEMP_SQL_FILE}
\x
CREATE USER root WITH PASSWORD 'testpassword';
GRANT ALL PRIVILEGES ON DATABASE postgres TO root;
EOF
sudo su postgres -c "psql -f ${TEMP_SQL_FILE}"

#bin/sh
cd ~
mkdir postgres
cd postgres

sudo apt-get update
sudo apt-get -y install postgresql-client

DBIP=$(cat /etc/hadoop/conf.empty/hadoop-env.sh | grep DBIP | sed 's/export //g' | sed 's/"//g')
eval $DBIP

PGPASSWORD=qazxcvbnm psql -h $DBIP -d postgres -U postgres -c "SELECT * FROM anomalies ORDER BY day_start;"
PGPASSWORD=qazxcvbnm psql -h $DBIP -d postgres -U postgres -c "SELECT * FROM crimes ORDER BY(year, month);"

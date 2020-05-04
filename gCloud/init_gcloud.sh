# CLOUD SHELL
# Create new database (only if database does not exist)
gcloud sql instances create db --database-version=POSTGRES_11 \
    --cpu=1 --memory=4GB \
    --zone=europe-west3-c \
    --storage-size 10 \
    --root-password=qazxcvbnm \
    --authorized-networks=0.0.0.0/0

gcloud sql connect postgres --user=postgres
#################################################
###### POSTGRESQL SCRIPT - CREATE DATABASE ######
#################################################
# CREATE TABLE Crimes (
# 	YEAR  INT NOT NULL,
# 	MONTH INT NOT NULL,
#   DISTRICT VARCHAR(10) NOT NULL,
#   CATEGORY VARCHAR(10) NOT NULL,
#   TOTAL INT NOT NULL,
#   DOMESTIC INT NOT NULL,
#   ARRESTED INT NOT NULL,
#   FBI INT NOT NULL,
#   PRIMARY KEY (YEAR, MONTH, DISTRICT, CATEGORY)
# );
# CREATE TABLE Anomalies (
# 	day_start DATE NOT NULL,
# 	day_end DATE NOT NULL,
#   DISTRICT VARCHAR(10) NOT NULL,
#   TOTAL INT NOT NULL,
#   FBI INT NOT NULL,
#   PERCENT FLOAT NOT NULL,
#   PRIMARY KEY (day_start, day_end, DISTRICT)
# );

DBIP=$(gcloud sql instances describe db --format='get(ipAddresses[0].ipAddress)')

gcloud beta dataproc clusters create cluster-7da8 \
--enable-component-gateway --bucket storage_bigdata \
--region europe-west3 --subnet default --zone europe-west3-c \
--master-machine-type n1-standard-2 --master-boot-disk-size 50 \
--num-workers 2 \
--worker-machine-type n1-standard-2 --worker-boot-disk-size 50 \
--image-version 1.3-deb9 \
--optional-components ANACONDA,JUPYTER,ZEPPELIN,ZOOKEEPER \
--project bigdata-256513 --max-age=12h \
--metadata "run-on-master=true" \
--initialization-actions gs://goog-dataproc-initialization-actions-europe-west3/kafka/kafka.sh \
--properties=hadoop-env:DBIP=$DBIP

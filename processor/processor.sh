
DBIP=$(cat /etc/hadoop/conf.empty/hadoop-env.sh | grep DBIP | sed 's/export //g' | sed 's/"//g')
eval $DBIP

cd ~
mkdir -p scripts
mkdir -p scripts/processor
cd scripts/processor
rm -f processor-1.0.jar
rm -f postgresql-42.2.12.jar
hadoop fs -copyToLocal gs:/jars/processor-1.0.jar .
hadoop fs -copyToLocal gs:/jars/postgresql-42.2.12.jar .
export SPARK_HOME="/usr/lib/spark"

$SPARK_HOME/bin/spark-submit --jars postgresql-42.2.12.jar --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 --class processor.Processor processor-1.0.jar $DBIP 7 40

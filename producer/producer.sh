###################################
###### Import data to bucket ######
###################################
# cd ~
# mkdir crimes/
# cd crimes
# wget http://www.cs.put.poznan.pl/kjankiewicz/bigdata/stream_project/crimes-in-chicago_result.zip
# unzip crimes-in-chicago_result
# rm *.zip
# mv crimes-in-chicago_result/* ./
# rmdir crimes-in-chicago_result
# wget http://www.cs.put.poznan.pl/kjankiewicz/bigdata/stream_project/Chicago_Police_Department_-_Illinois_Uniform_Crime_Reporting__IUCR__Codes.csv
# hadoop fs -moveFromLocal * gs:/crimes/

##########################################
######## Import data from bucket #########
##########################################
##########################################
######         Producer jar         ######
######     (KafkaProducer.jar)      ######
###### should be uploaded to backet ######
##########################################

cd ~
mkdir -p crimes
cd crimes

hadoop fs -copyToLocal gs:/crimes/* .
mkdir crimes
mv part* crimes/
hadoop fs -mkdir /crimes
hadoop fs -copyFromLocal Chicago_Police_Department_-_Illinois_Uniform_Crime_Reporting__IUCR__Codes.csv /cimes
cd ..

rm -f producer-1.0.jar
hadoop fs -copyToLocal gs:/jars/producer-1.0.jar .

# # Check kafka topics
# /usr/lib/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --list

# Create new topic
/usr/lib/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic raw

CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
java -cp /usr/lib/kafka/libs/*:producer-1.0.jar producer.TestProducer crimes/crimes 15 raw 1 ${CLUSTER_NAME}-w-0:9092


# DELETE TOPIC 
# /usr/lib/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic raw



# Simple producer
# CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
# /usr/lib/kafka/bin/kafka-console-producer.sh \
#  --broker-list ${CLUSTER_NAME}-w-0:9092 \
#  --topic raw
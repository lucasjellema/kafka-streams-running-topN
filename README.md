# kafka-streams-running-topN
Running Top N aggregation using Apache Kafka Streams

Details on this application are discussed in the following blog articles:

* Apache Kafka Streams – Running Top-N Aggregation grouped by Dimension – from and to Kafka Topic -  https://technology.amis.nl/2017/02/12/apache-kafka-streams-running-top-n-grouped-by-dimension-from-and-to-kafka-topic/

* Getting Started with Kafka Streams – building a streaming analytics Java application against a Kafka Topic - https://technology.amis.nl/2017/02/11/getting-started-with-kafka-streams-building-a-streaming-analytics-java-application-against-a-kafka-topic/

* NodeJS – Publish messages to Apache Kafka Topic with random delays to generate sample events based on records in CSV file - https://technology.amis.nl/2017/02/09/nodejs-publish-messages-to-apache-kafka-topic-with-random-delays-to-generate-sample-events-based-on-records-in-csv-file/

* Workshop Apache Kafka – presentation and hands on labs for getting started - https://technology.amis.nl/2017/02/10/workshop-apache-kafka-presentation-and-hands-on-labs-for-getting-started/

Three applications in this repository:

kafka-node-countries: Node.JS application  (with kafka-node) that produces country messages from a CSV file with countries to the Kafka Topic countries

kafka-node-TopN-Report: Node.JS application (with kafka-node) that produces a periodic report on the current Top N (largest countries per continent) based on the Kafka Topic Top3CountrySizePerContinent

Kafka-Streams-Country-TopN: Java application (with Kafka Streams) that produces the running Top 3 standings per continent of the largest countries, published to Kafka Topic Top3CountrySizePerContinent


To run these applications, go through the following steps:

1. git clone https://github.com/lucasjellema/kafka-streams-running-topN

2. kafka-node-countries

* cd kafka-node-countries
* npm install
* node KafkaCountryProducer.js


3. Kafka-Streams-Country-TopN

* cd Kafka-Streams-Country-TopN
* mvn package
* mvn install dependency:copy-dependencies
* java -cp target/Kafka-Streams-Country-TopN-1.0-SNAPSHOT.jar;target/dependency/* nl.amis.streams.countries.App 


4. Kafka-Streams-Country-TopN

* cd kafka-node-TopN-Report
* npm install
* node KafkaCountryStreamsConsumer.js



Note: the following prequisites have to be met:
* a running Apache Kafka Cluster is available. The sources assume ubuntu:9092 (Broker) and ubuntu:2181 (ZooKeeper); 
* Java 8 JRE is installed (1.8.x)
* Apache Maven is installed (3.x)
* Node.JS is installed (6.x or higher)
* npm is installed
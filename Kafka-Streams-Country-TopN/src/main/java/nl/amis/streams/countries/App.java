package nl.amis.streams.countries;

import nl.amis.streams.JsonPOJOSerializer;
import nl.amis.streams.JsonPOJODeserializer;

// generic Java imports
import java.util.Properties;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;
// Kafka imports
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;
// Kafka Streams related imports
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

public class App {
    static public class CountryMessage {
        /* the JSON messages produced to the countries Topic have this structure:
         { "name" : "The Netherlands"
         , "code" : "NL
         , "continent" : "Europe"
         , "population" : 17281811
         , "size" : 42001
         };
  
        this class needs to have at least the corresponding fields to deserialize the JSON messages into
        */

        public String code;
        public String name;
        public int population;
        public int size;
        public String continent;
    }

    static public class CountryTop3 {

       public  CountryMessage[]  nrs = new CountryMessage[4] ;
       public CountryTop3() {}
    }



    private static final String APP_ID = "countries-topn-streaming-analysis-app";

    public static void main(String[] args) {
        System.out.println("Kafka Streams Demonstration");

        // Create an instance of StreamsConfig from the Properties instance
        StreamsConfig config = new StreamsConfig(getProperties());
        final Serde < String > stringSerde = Serdes.String();
        final Serde < Long > longSerde = Serdes.Long();

        // define countryMessageSerde
        Map < String, Object > serdeProps = new HashMap < String, Object > ();
        final Serializer < CountryMessage > countryMessageSerializer = new JsonPOJOSerializer < > ();
        serdeProps.put("JsonPOJOClass", CountryMessage.class);
        countryMessageSerializer.configure(serdeProps, false);

        final Deserializer < CountryMessage > countryMessageDeserializer = new JsonPOJODeserializer < > ();
        serdeProps.put("JsonPOJOClass", CountryMessage.class);
        countryMessageDeserializer.configure(serdeProps, false);
        final Serde < CountryMessage > countryMessageSerde = Serdes.serdeFrom(countryMessageSerializer, countryMessageDeserializer);

        // define countryTop3Serde
        serdeProps = new HashMap<String, Object>();
        final Serializer<CountryTop3> countryTop3Serializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", CountryTop3.class);
        countryTop3Serializer.configure(serdeProps, false);

        final Deserializer<CountryTop3> countryTop3Deserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", CountryTop3.class);
        countryTop3Deserializer.configure(serdeProps, false);
        final Serde<CountryTop3> countryTop3Serde = Serdes.serdeFrom(countryTop3Serializer, countryTop3Deserializer );

        // building Kafka Streams Model
        KStreamBuilder kStreamBuilder = new KStreamBuilder();
        // the source of the streaming analysis is the topic with country messages
        KStream<String, CountryMessage> countriesStream = 
                                       kStreamBuilder.stream(stringSerde, countryMessageSerde, "countries");


        // THIS IS THE CORE OF THE STREAMING ANALYTICS:
        // top 3 largest countries per continent, published to topic Top3CountrySizePerContinent
        KTable<String,CountryTop3> top3PerContinent = countriesStream
                      // the dimension for aggregation is continent; assign the continent as the key for each message
                      .selectKey((k, country) -> country.continent)
                      // for each key value (every continent in the stream) perform an aggregation
                      .aggregateByKey( 
                          // first initialize a new CountryTop3 object, initially empty
                          CountryTop3::new
                      , // for each country in the continent, invoke the aggregator, passing in the continent, the country element and the CountryTop3 object for the continent 
                         (continent, countryMsg, top3) -> {
                                   // add the new country as the last element in the nrs array
                                   top3.nrs[3]=countryMsg;
                                   //  sort the array by country size, largest first
                                   Arrays.sort(
                                       top3.nrs, (a, b) -> {
                                         // in the initial cycles, not all nrs element contain a CountryMessage object  
                                         if (a==null)  return 1;
                                         if (b==null)  return -1;
                                         // with two proper CountryMessage objects, do the normal comparison
                                         return Integer.compare(b.size, a.size);
                                       }
                                   );
                                   // lose nr 4, only top 3 is relevant
                                   top3.nrs[3]=null;
                           return (top3);
                        }
                        ,  stringSerde, countryTop3Serde
                        ,  "Top3LargestCountriesPerContinent"
                       );

        // prepare Top3 messages to be printed to the console
        top3PerContinent.<String>mapValues((top3) -> {
              String rank = " 1. "+top3.nrs[0].name+" - "+top3.nrs[0].size                                   
                          + ((top3.nrs[1]!=null)? ", 2. "+top3.nrs[1].name+" - "+top3.nrs[1].size:"")
                          + ((top3.nrs[2]!=null) ? ", 3. "+top3.nrs[2].name+" - "+top3.nrs[2].size:"")
              ;                                 
              return "List for "+ top3.nrs[0].continent +rank;
          }  
        )
             .print(stringSerde,stringSerde);
        // publish the Top3 messages to Kafka Topic Top3CountrySizePerContinent     
        top3PerContinent.to(stringSerde, countryTop3Serde,  "Top3CountrySizePerContinent");

        System.out.println("Starting Kafka Streams Countries Example");
        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, config);
        kafkaStreams.start();
        System.out.println("Now started CountriesStreams Example");
    }

    private static Properties getProperties() {
        Properties settings = new Properties();
        // Set a few key parameters
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        // Kafka bootstrap server (broker to talk to); ubuntu is the host name for my VM running Kafka, port 9092 is where the (single) broker listens 
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "ubuntu:9092");
        // Apache ZooKeeper instance keeping watch over the Kafka cluster; ubuntu is the host name for my VM running Kafka, port 2181 is where the ZooKeeper listens 
        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "ubuntu:2181");
        // default serdes for serialzing and deserializing key and value from and to streams in case no specific Serde is specified
        settings.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        settings.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        settings.put(StreamsConfig.STATE_DIR_CONFIG, "C:\\temp");
        // to work around exception Exception in thread "StreamThread-1" java.lang.IllegalArgumentException: Invalid timestamp -1
        // at org.apache.kafka.clients.producer.ProducerRecord.<init>(ProducerRecord.java:60)
        // see: https://groups.google.com/forum/#!topic/confluent-platform/5oT0GRztPBo
        settings.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return settings;
    }

}
package com.smk.kafka.health;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import com.jayway.jsonpath.JsonPath;
import net.minidev.json.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

import com.fasterxml.jackson.databind.JsonNode;

public class SubTopicsFromKStream {

	public static void main(String[] args) {
		
		Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "health-care-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
        //config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        // Exactly once processing!!
        //config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        
        // json Serde
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
        
        StreamsBuilder builder = new StreamsBuilder();
        
        System.out.println("===========================================================1");

        KStream<String, String> consolidatedJson = builder.stream("Auth-Topic",Consumed.with(null,Serdes.String()));
        consolidatedJson.print(Printed.toSysOut());
        //String jsonpathCreatorNamePath = "$.[Subscriber,Patient,Case,Service]";
        
        System.out.println("===========================================================2");
        
		
		KStream<String, String> peek = consolidatedJson.peek((key,value) -> System.out.println("key "+key+"  value "+value));
        
        consolidatedJson.to("dummy-node1");


        KStream<String, Object> stringObjectKStream = consolidatedJson.flatMapValues(value -> {
            Object read1 = JsonPath.read(value.toString(), "$.Subscriber");
            Object read2 = JsonPath.read(value.toString(), "$.Patient");
            Object read3 = JsonPath.read(value.toString(), "$.Cases");
            Object read4 = JsonPath.read(value.toString(), "$.Service");
            System.out.println("*******************");
            System.out.println(Arrays.asList(read1, read2, read3, read4));
            System.out.println("####################");
            String s = new JSONObject((Map<String, ?>) read1).toString();
            System.out.println(s);
            return Arrays.asList(read1, read2, read3, read4);
        });

        KStream<String, String> stringStringKStream = stringObjectKStream.mapValues(value -> {
            String s = new JSONObject((Map<String, ?>) value).toString();
            return s;
        });

        KStream<String, String>[] branch = stringStringKStream.branch(
                (key, value) -> value.contains("MEM_ID"),
                (key, value) -> value.contains("PAT_ID"),
                (key, value) -> value.contains("CASE_TYPE"),
                (key, value) -> value.contains("SVC_ID"));

        branch[0].to("Subscriber");
        branch[1].to("Patient");
        branch[2].to("Cases");
        branch[3].to("Service");

        branch[0].peek((key,value) ->
                System.out.println("key "+key+"  value "+value));

        System.out.println("===========================================================3");
        
        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp();
        streams.start();
        
        System.out.println("===========================================================4");

        // print the topology
        streams.localThreadsMetadata().forEach(data -> System.out.println(data));

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

	}

}

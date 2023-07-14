package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class WordCountUsingStreams {

    public static void main(String[] args){
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,"wordcount-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // 1 --> Stream for kafka                                                        <null, "Kafka Streams For Kafka">
        KStream<String,String> wordCountInput = builder.stream("word-count-input");

        // 2 --> Map values to lowercase                                                 <null, "kafka streams for kafka">
        KTable<String,Long> wordCounts = wordCountInput
                .mapValues(textLine -> textLine.toLowerCase())
                // 3 -->Flatmap values, split by space                                           <null,"kafka">,<null,"streams">,<null,"for">,<null,"kafka">
                .flatMapValues(lowerCaseTextLine -> Arrays.asList(lowerCaseTextLine.split(" ")))
                // 4 --> select a key to apply a key (we discard the old key)                    <"kafka","kafka">,<"streams","streams">,<"for","for">,<"kafka","kafka">
                .selectKey((ignoredKey,word) -> word)
                // 5 --> group by key before aggregation                                         (<"kafka","kafka">,<"kafka","kafka">),(<"streams","streams">),(<"for","for">)
                .groupByKey()
                // 6 --> count occurrences in each group                                         <"kafka",2>,<"streams",1>,<"for",1>
                .count(Named.as("Counts"));

        // 7 --> To in order to write the results back to kafka                          data point is written back to kafka
        String outputTopic = "word-count-output";
        wordCounts.toStream()
                .to(outputTopic, Produced.with(Serdes.String(),Serdes.Long()));

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology,config);

        streams.start();


        System.out.println(streams);

        //Shutdown hook to correctly close the stream application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

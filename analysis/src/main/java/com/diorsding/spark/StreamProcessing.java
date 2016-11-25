package com.diorsding.spark;


import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

/**
 * Spark 2.0 + Kafka 0.10 Integration
 * https://people.apache.org/~pwendell/spark-nightly/spark-master-docs/latest/streaming-kafka-0-10-integration.html
 *
 * @author jiashan
 *
 */
public class StreamProcessing {



    @SuppressWarnings("serial")
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName(StreamProcessing.class.getSimpleName());
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

        Collection<String> topics = Arrays.asList("nytimes");
        Map<String, Object> kafkaParams = getKafkaParams();

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        JavaPairDStream<String, String> javaPairDtream =
                stream.mapToPair(new PairFunction<ConsumerRecord<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(ConsumerRecord<String, String> record) throws Exception {

                        // convert to <docID, article>
                return new Tuple2<String, String>(record.key(), record.value());
            }
        });

        PipelineModel kMeansModel = PipelineModel.load("/model-location");

        // data type should be same. Need dataset<Row>
        // kMeansModel.transform(javaPairDtream)


        javaPairDtream.print();


        jssc.start();
        jssc.awaitTermination();
    }

    public static Map<String, Object> getKafkaParams() {
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("bootstrap.servers", "10.64.215.78:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "testConsumerGroup");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", true);

        return kafkaParams;
    }

}

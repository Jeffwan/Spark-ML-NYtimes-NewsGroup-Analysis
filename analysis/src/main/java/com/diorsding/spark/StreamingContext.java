package com.diorsding.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class StreamingContext {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName(StreamingContext.class.getSimpleName());

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, null);

        // KafkaUtils.crea

    }

}

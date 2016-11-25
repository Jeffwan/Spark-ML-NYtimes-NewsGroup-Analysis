package com.diorsding.spark;

import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

public class ClusterProcessing {

    public static void main(String[] args) throws IOException {
        Logger.getLogger("org.apache.spark").setLevel(Level.INFO);

        // Deprecated Solution
        JavaSparkContext jsc = MongoCommonIngestion.getJavaSparkContext();
        SQLContext sqlContext = new SQLContext(jsc);
        JavaPairRDD<String, String> docIdArticleRdd = MongoCommonIngestion.getMongoDocumentData(jsc);
        Dataset<Tuple2<String, String>> docIdArticleDF =
                sqlContext.createDataset(docIdArticleRdd.rdd(), Encoders.tuple(Encoders.STRING(), Encoders.STRING()));

        // SparkSession sparkSession = getSparkSession();
        // sparkSession.createdataframe

        docIdArticleDF.cache();

        PipelineModel model = trainModelByPipeline(docIdArticleDF);

        model.write().overwrite().save("/tmp/spark-news-anaylysis");

        Dataset<Row> rows = model.transform(docIdArticleDF);
        rows.printSchema();
    }

    private static PipelineModel trainModelByPipeline(Dataset<Tuple2<String, String>> docIdArticleDF) {
        // TODO: raw data format [_1, _2]. update column name
        // Convert article to array<word>
        Tokenizer tokenizer = new Tokenizer().setInputCol("_2").setOutputCol("words");

        // Remove meaningless words
        StopWordsRemover remover = new StopWordsRemover().setInputCol("words").setOutputCol("noStopWrods");

        // Converts those words into fixed-length feature vectors -- (20,[0,4,6,13..]
        HashingTF hashingTF = new HashingTF().setInputCol("noStopWrods").setOutputCol("rawFeatures").setNumFeatures(20);

        IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("idf");

        // normalizing each vector to have unit norm
        // It simply gets all of the data on the same scale - If we don't normalize, an article with more words would be
        // weighted differently
        Normalizer normalizer = new Normalizer().setInputCol("idf").setOutputCol("features");

        // Prediction range -> [1, 10]
        KMeans kMeans = new KMeans().setFeaturesCol("features").setPredictionCol("prediction").setK(10).setSeed(0);

        Pipeline pipeline =
                new Pipeline().setStages(new PipelineStage[] {tokenizer, remover, hashingTF, idf, normalizer, kMeans});
        PipelineModel model = pipeline.fit(docIdArticleDF);

        return model;
    }
}

package com.diorsding.spark;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
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
import org.bson.Document;

import scala.Tuple2;

import com.google.common.base.Joiner;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

public class MongoConnector {

    private static final String mongoHost = "10.64.215.78";


    public static void main(String[] args) throws IOException {
        // TODO: use sparksession instead. - has Mongo Dependency
        JavaSparkContext jsc = getJavaSparkContext();
        SQLContext sqlContext = new SQLContext(jsc);
        Logger.getLogger("org.apache.spark").setLevel(Level.INFO);

        // TODO: use schema instead
        JavaPairRDD<String, String> docIdArticleRdd = getMongoDocumentData(jsc);
        Dataset<Tuple2<String, String>> docIdArticleDF =
                sqlContext.createDataset(docIdArticleRdd.rdd(), Encoders.tuple(Encoders.STRING(), Encoders.STRING()));

        docIdArticleDF.cache();

        PipelineModel model = trainModelByPipeline(docIdArticleDF);

        model.write().overwrite().save("/tmp/spark-news-anaylysis");

        Dataset<Row> rows = model.transform(docIdArticleDF);
        rows.printSchema();

        showResult(model, docIdArticleDF);
    }

    public static void showResult(PipelineModel model, Dataset<Tuple2<String, String>> docIdArticleDF) {

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

    private static JavaPairRDD<String, String> getMongoDocumentData(JavaSparkContext jsc) {
        // Customized Configuration
        Map<String, String> readOverrides = new HashMap<String, String>();
        readOverrides.put("collection", "news");
        readOverrides.put("readPreference.name", "secondaryPreferred");
        ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);

        // Get all documents in the specific collection
        JavaMongoRDD<Document> customRdd = MongoSpark.load(jsc, readConfig);

        // Filter sub sets on Mongo level before sending to Spark to do filter.
        JavaMongoRDD<Document> aggregatedRdd =
                customRdd.withPipeline(Collections.singletonList(Document.parse("{ $match: { category : 'asia' } }")));

        // Rdd -> JavaPairRdd<docId, article>
        JavaPairRDD<String, String> docIdArticleRdd =
                aggregatedRdd.mapToPair(new PairFunction<Document, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Document document) throws Exception {
                        // To make rdd -> dataset easy, we use string here. ObjectId can be constructed.
                        String docId = document.getObjectId("_id").toString();

                        // Mongo Stores long text as List<String>
                        List<String> articleList = (List<String>) document.get("article");
                        String article = Joiner.on("").join(articleList);
                        // This could be skipped, we use tokenlizer to do this
                        article = article.toLowerCase().replaceAll("[.,!?\n]", " ");

                        return new Tuple2<String, String>(docId, article);
                    }
                });

        // docIdArticleRdd.saveAsTextFile("/tmp/docIdArticleRddFile");
        return docIdArticleRdd;
    }

    @SuppressWarnings({"resource", "unused"})
    private static void dropDatabase(final String connectionString) {
        MongoClientURI uri = new MongoClientURI(connectionString);
        new MongoClient(uri).dropDatabase(uri.getDatabase());
    }

    private static JavaSparkContext getJavaSparkContext() {
        SparkConf sparkConf =
                new SparkConf()
                        .setMaster("local[2]")
                        .setAppName(MongoConnector.class.getSimpleName())
                        .set("spark.mongodb.input.uri", String.format("mongodb://%s:27017/NYtimes.newscopy", mongoHost))
                        .set("spark.mongodb.output.uri",
                                String.format("mongodb://%s:27017/NYtimes.newscopy", mongoHost));

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        return jsc;
    }

}

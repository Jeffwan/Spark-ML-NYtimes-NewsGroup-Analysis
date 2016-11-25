package com.diorsding.spark;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import scala.Tuple2;

import com.google.common.base.Joiner;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

public class MongoCommonIngestion {

    public static final String mongoHost = "10.64.215.78";

    public static JavaSparkContext getJavaSparkContext() {
        SparkConf sparkConf =
                new SparkConf()
                        .setMaster("local[2]")
                        .setAppName(ClusterProcessing.class.getSimpleName())
                        .set("spark.mongodb.input.uri", String.format("mongodb://%s:27017/NYtimes.newscopy", mongoHost))
                        .set("spark.mongodb.output.uri",
                                String.format("mongodb://%s:27017/NYtimes.newscopy", mongoHost));

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        return jsc;
    }

    public static SparkSession getSparkSession() {
        SparkSession sparkSession =
                SparkSession
                        .builder()
                        .appName(ClusterProcessing.class.getSimpleName())
                        .config("spark.mongodb.input.uri",
                                String.format("mongodb://%s:27017/NYtimes.newscopy", mongoHost))
                        .config("spark.mongodb.output.uri",
                                String.format("mongodb://%s:27017/NYtimes.newscopy", mongoHost)).getOrCreate();

        return sparkSession;
    }

    public static JavaPairRDD<String, String> getMongoDocumentData(JavaSparkContext jsc) {
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
    public static void dropDatabase(final String connectionString) {
        MongoClientURI uri = new MongoClientURI(connectionString);
        new MongoClient(uri).dropDatabase(uri.getDatabase());
    }
}

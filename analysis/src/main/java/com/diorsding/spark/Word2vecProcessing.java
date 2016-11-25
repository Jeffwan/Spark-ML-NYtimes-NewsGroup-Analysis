package com.diorsding.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class Word2vecProcessing {

    private static final String mongoHost = "10.64.215.78";

    public static void main(String[] args) {
        Logger.getLogger("org.apache.spark").setLevel(Level.INFO);

        // Deprecated Solution
        JavaSparkContext jsc = MongoCommonIngestion.getJavaSparkContext();
        SQLContext sqlContext = new SQLContext(jsc);
        JavaPairRDD<String, String> docIdArticleRdd = MongoCommonIngestion.getMongoDocumentData(jsc);
        Dataset<Tuple2<String, String>> docIdArticleDF =
                sqlContext.createDataset(docIdArticleRdd.rdd(), Encoders.tuple(Encoders.STRING(), Encoders.STRING()));

        Word2Vec word2Vec = new Word2Vec().setInputCol("_2").setOutputCol("synonyms").setVectorSize(3).setMinCount(0);
        Word2VecModel word2VecModel = word2Vec.fit(docIdArticleDF);

        Dataset<Row> synonymsWords = word2VecModel.findSynonyms("test word", 5);

        for (Row r : synonymsWords.select("result").takeAsList(3)) {
            System.out.println(r);
        }

        System.out.println("-----------------------------");

        List<Row> data = Arrays.asList(RowFactory.create(Arrays.asList("Hi I heard about Spark".split(" "))));

        StructType schema =
                new StructType(new StructField[] {new StructField("text", new ArrayType(DataTypes.StringType, true),
                        false, Metadata.empty())});


        Dataset<Row> documentDF = MongoCommonIngestion.getSparkSession().createDataFrame(data, schema);
        Dataset<Row> result = word2VecModel.transform(documentDF);
        for (Row r : result.select("result").takeAsList(3)) {
            System.out.println(r);
        }

    }
}

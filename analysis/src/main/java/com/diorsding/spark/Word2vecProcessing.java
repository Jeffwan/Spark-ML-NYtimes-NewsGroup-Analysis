package com.diorsding.spark;

import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Word2vecProcessing {

    public static void main(String[] args) {
        SparkSession sparkSession =
                SparkSession.builder().appName(Word2vecProcessing.class.getSimpleName()).getOrCreate();

        Dataset<Row> documentDF = null;

        Word2Vec word2Vec = new Word2Vec().setInputCol("text").setOutputCol("text").setVectorSize(3).setMinCount(0);

        Word2VecModel model = word2Vec.fit(documentDF);
        Dataset<Row> result = model.transform(documentDF);

    }

}

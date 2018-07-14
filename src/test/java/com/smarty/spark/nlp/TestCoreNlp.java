package com.smarty.spark.nlp;

import static com.ziad.spark.corenlp.functions.*;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.*;
import org.junit.Test;

public class TestCoreNlp {
    @Test
    public void test() {
	SparkSession session = null;
	try {
	    session = SparkSession.builder().master("local").appName("NlpSpark").getOrCreate();
	    List<String> data = Arrays.asList("I wrote this program. Hope It helps",
		    "This is a test sentence");
	    Dataset<Row> df = session.createDataset(data, Encoders.STRING()).toDF();
	    df.printSchema();
	    df.select(ner.apply(functions.col("value"))).show();
	    df.select(ner.apply(functions.col("value"))).collectAsList().forEach(System.out::println);
	    df.select(sentiment.apply(functions.col("value"))).collectAsList().forEach(System.out::println);
	    System.out.println("\n------------------------------\n");
	    df.select(lemmas.apply(functions.col("value"))).show();

	    df.select(sentiment.apply(functions.col("value"))).collectAsList().forEach(System.out::println);
	    System.out.println("\n------------------------------\n");
	    df.select(lemmas.apply(functions.col("value"))).show();

	    df.select(sentiment.apply(functions.col("value"))).collectAsList().forEach(System.out::println);
	    System.out.println("\n------------------------------\n");
	    df.select(sentiment.apply(functions.col("value"))).collectAsList().forEach(System.out::println);
	    df.select(lemmas.apply(functions.col("value"))).show();
	} finally {
	    if (session != null)
		session.stop();
	}
    }
}

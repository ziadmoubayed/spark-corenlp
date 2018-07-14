package com.ziad.spark.corenlp;

import static org.apache.spark.sql.functions.udf;

import java.util.Arrays;
import java.util.Properties;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.simple.Document;
import edu.stanford.nlp.simple.Sentence;
import edu.stanford.nlp.trees.Tree;

/**
 * Static methods
 */
public class functions {
    /**
    *	
    */
    private static transient StanfordCoreNLP pipeline;

    /**
     * Splits a text into sentences.
     */
    public static UserDefinedFunction ssplit = udf(
	    (UDF1<String, String[]>) text -> Arrays.stream((new Document(text).sentences()).toArray(new Sentence[0]))
		    .map(s -> s.text()).toArray(String[]::new),
	    DataTypes.createArrayType(DataTypes.StringType));

    /**
     * Tokenizes a text into words.
     */
    public static UserDefinedFunction tokenize = udf(
	    (UDF1<String, String[]>) s -> new Sentence(s).words().toArray(new String[0]),
	    DataTypes.createArrayType(DataTypes.StringType));

    /**
     * Generates the named entity tags of the text.
     */
    public static UserDefinedFunction ner = udf(
	    (UDF1<String, String[]>) text -> new Sentence(text).nerTags().toArray(new String[0]),
	    DataTypes.createArrayType(DataTypes.StringType));

    /**
     * Measures the sentiment of an input sentence on a scale of 0 (strong
     * negative) to 4 (strong positive).
     */
    public static UserDefinedFunction sentiment = udf((UDF1<String, Integer>) s -> {
	StanfordCoreNLP pipeline = getOrCreateSentimentPipeline();
	Annotation annotation = pipeline.process(s);
	Tree tree = annotation.get(CoreAnnotations.SentencesAnnotation.class).get(0)
		.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
	return RNNCoreAnnotations.getPredictedClass(tree);
    }, DataTypes.IntegerType);

    /**
     * Generates the word lemmas of the text.
     */
    public static UserDefinedFunction lemmas = udf(
	    (UDF1<String, String[]>) text -> new Sentence(text).lemmas().toArray(new String[0]),
	    DataTypes.createArrayType(DataTypes.StringType));

    /**
     * @return StanfordCoreNLP instance with annotators
     */
    private static synchronized StanfordCoreNLP getOrCreateSentimentPipeline() {
	if (pipeline == null) {
	    Properties props = new Properties();
	    props.put("annotators", "tokenize, ssplit, pos, parse, sentiment");
	    pipeline = new StanfordCoreNLP(props);
	}
	return pipeline;
    }
}

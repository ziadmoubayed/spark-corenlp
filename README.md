# spark-corenlp
Spark DataFrame wrapper methods for CoreNlp SimpleApi Annotators.
These methods were tested with spark 2.3.1 and standford version 3.9.1

To import the methods `import static com.ziad.spark.nlp.functions.*`.

* *`tokenize`*: Splits the text into roughly “words”, using rules or methods suitable for the language being processed.
* *`ssplit`*: Splits a sequence of tokens into sentences.
* *`lemmas`*: 	Generates the word lemmas for all tokens in the corpus.
* *`ner`*: Generates the named entity tags of the text.
* *`sentiment`*: Measures the sentiment of an input sentence on a scale of 0 (strong negative) to 4 (strong positive).

Note: You need to add the core nlp models jar to your class path.
LANGUAGE	VERSION
[Arabic](http://nlp.stanford.edu/software/stanford-arabic-corenlp-2018-02-27-models.jar)		3.9.1
Chinese		3.9.1
English		3.9.1
English (KBP)	.9.1
French		3.9.1
German		3.9.1
Spanish		3.9.1


Example of usage:
~~~Java
//Collection of Strings (text) to parse...
List<String> data = Arrays.asList("first text", 
"second text");
/*
1. create Dataset from String collection
2. call UserDefinedFunction, named "sentiment" to measure the sentiment of an input sentence
3. generate sentiment type using sentiment scale
4. print table with results
 */
 Dataset<Row> df = session.createDataset(data, Encoders.STRING()).toDF();
 df.select(col("value"), sentiment.apply(col("value")).as("sentiment"))
 .show();

Output:

+-----------+----------+
|      value| sentiment|
+-----------+----------+
| first text|         2|
|second text|         2|
+-----------+----------+

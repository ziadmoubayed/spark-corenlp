# spark-corenlp
Spark DataFrame wrapper methods for CoreNlp SimpleApi Annotators.
These methods were tested with spark 2.3.1 and standford version 3.9.1

To import the methods `import static com.ziad.spark.nlp.functions.*`.

* *`tokenize`*: Tokenizes a text into words.
* *`ssplit`*: Splits a text into sentences.
* *`lemmas`*: Generates the word lemmas of the text.
* *`ner`*: Generates the named entity tags of the text.
* *`sentiment`*: Measures the sentiment of an input sentence on a scale of 0 (strong negative) to 4 (strong positive).

Note: You need to add the core nlp models jar to your class path.

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

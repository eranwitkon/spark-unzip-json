# spark-unzip-json
Demonstrate how to use Spark & Scala to extract a GZIP JSON within JSON

## How to use this demo?

Best way to use this demo is by taking the JAR from the artifacts directory and using spark-submit
``` $SPARK_HOME/bin/spark-submit --master <your master URL> --class extractJSONDriver <path to jar file> <arguments> ```

The arguments to this app are:
<br>
```source-dir``` - the directory to read the JSON files from

```dest dir``` - the directory to write the output files to

```output options``` - JSON, parquet, test.<br> 
JSON and parquet will generate the outut using the specified format. test will not generate output file and only write out the schema

 The other option is to use ```spark-shell``` and paste the scala code in and let it run.
 for this to work, make sure you don't paste ```extractJSONDriver``` code which creates ```SparkContext``` & ```SQLContext```
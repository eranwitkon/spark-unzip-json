# spark-unzip-json
Demonstrate how to use Spark & Scala to extract a GZIP JSON within JSON

This code shows how to take a compressed JSON field within a JSON file, extract is and generate a nested JSON.

* Source schema *

```root
 |-- cty: string (nullable = true)
 |-- gzip: string (nullable = true)
 |-- nm: string (nullable = true)
 |-- yrs: string (nullable = true)
```
* Source data sample *
```
{"cty":"United Kingdom","gzip":"H4sIAAAAAAAAAKtWystVslJQcs4rLVHSUUouqQTxQvMyS1JTFLwz89JT8nOB4hnFqSBxj/zS4lSF/DQFl9S83MSibKBMZVExSMbQwNBM19DA2FSpFgDvJUGVUwAAAA==","nm":"Cnut","yrs":"1016-1035"}
```

* Dest schema *

```
root
 |-- cty: string (nullable = true)
 |-- extractedJson: struct (nullable = true)
 |    |-- cty: string (nullable = true)
 |    |-- hse: string (nullable = true)
 |    |-- nm: string (nullable = true)
 |    |-- yrs: string (nullable = true)
 |-- nm: string (nullable = true)
 |-- yrs: string (nullable = true)
```

* Dest data sample *
```
{"cty":"United Kingdom","extractedJson":{"cty":"United Kingdom","hse":"House of Denmark","nm":"Cnut","yrs":"1016-1035"},"nm":"Edmund lronside","yrs":"1016"}
```

## How to use this demo?

** Note This code works with the provided sample files, to use with other files ExtractJSON should be changed **

Best way to use this demo is by taking the JAR from the artifacts directory and using spark-submit:

### Running using spark-submit:

 <br>
``` $SPARK_HOME/bin/spark-submit --master <your master URL> --class org.witkon.extractJSON.extractJSONDriver <path to jar file> <arguments> ```

The ```<arguments>``` to this app are:
<br>
```source-dir``` - the directory to read the JSON files from

```dest dir``` - the directory to write the output files to

```output options``` - JSON, parquet, test.<br> 
JSON and parquet will generate the outut using the specified format. test will not generate output file and only write out the schema

### Example

```$SPARK_HOME/bin/spark-submit --master local[2] --class org.witkon.extractJSON.extractJSONDriver /home/eranw/sparkJSONSample/sparkJsonSample.jar /home/eranw/Workspace/sparkJsonSample/gzipSample /home/eranw/Workspace/sparkJsonSample/extractedOutput test```

### Running using spark-shell:
 The other option is to run ```spark-shell``` 
  click ```:p```
  and paste the code in file `spark-shell-testing.scala` 
  then ```Ctrl-D```
  
  Make sure you update both ```sourceFilesPath``` and ```destFilePath``` before pasting it to ```spark-shell```
  
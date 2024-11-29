# Week 6 Infrastructure Track Spark Training



## Usage Day 1

To launch the spark and iceberg docker containers, run:
- On Mac ```make up ```
- On Windows ```docker compose up -d ```

Then, you should be able to access a Jupyter notebook at `http://localhost:8888/tree/notebooks`.

The first notebook to be able to run is the `event_data_pyspark.ipynb` inside the `notebooks` folder.

## Usage Day 2

Set two environment variables:
 - `export DATA_ENGINEER_IO_WAREHOUSE=eczachly-academy-warehouse`
 - `export DATA_ENGINEER_IO_WAREHOUSE_CREDENTIAL=<credential I share with you>`

Download Spark 3.5 [here](https://www.apache.org/dyn/closer.lua/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz)
Unpack it at `bootcamp3/infrastructure/4-apache-spark-training`

Run 

```bash 
cd spark-3.5.0-bin-hadoop

bin/spark-submit \
 --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.0,software.amazon.awssdk:bundle:2.20.18 \
../jobs/connect_job

```



## FAQ: Troubleshooting Spark Issues

### Error: Could not find or load main class org.apache.spark.launcher.Main

**Symptom:** When running Spark on Debian WSL in Windows 10 with Java 17, you might encounter an error stating, "Error: Could not find or load main class org.apache.spark.launcher.Main."

**Solution:**
1. **Java Version Compatibility:** Spark may not be compatible with Java 17. Downgrading to Java 8 resolved the issue for some users. Ensure your `JAVA_HOME` variable is correctly set to the Java 8 directory, for example, `/usr/lib/jvm/java-8-openjdk-amd64`.
   
2. **Spark Version:** If you encounter issues with Spark 3.5, consider downgrading to Spark 3.0, as there might not be significant feature differences impacting your work.

### Missing Library Error: org/apache/spark/sql/catalyst/analysis/RewriteRowLevelCommand

**Symptom:** Running certain Spark jobs may throw a `java.lang.NoClassDefFoundError` related to missing libraries.

**Solution:**
- **Ensure Java and Spark Installation:** Make sure Java is installed in the actual Linux distribution rather than via Windows, and that `JAVA_HOME` is pointing to the correct installation directory, such as `/usr/lib/jvm/java-21-openjdk-amd64` for Java 21.
- **Check Spark and Java Version Compatibility:** Upgrading to Java 8 and using Spark 3.5 can solve this issue. It's crucial to set `JAVA_HOME` appropriately.

### Py4JJavaError on Mac

**Symptom:** Encountering a `Py4JJavaError` when attempting to initialize SparkSession, even with Java version 8 installed and the `JAVA_HOME` path correctly set.

**Solution:**
- **Correct `JAVA_HOME` Setting:** Verify `JAVA_HOME` is set to the correct location. Use "which javac" to find the Java installation directory and set `JAVA_HOME` to the parent folder of the path returned.
- **`SPARK_PATH` Update:** If encountering version conflicts, ensure `SPARK_PATH` in your `.bash_profile` or equivalent is pointing to the correct Spark version installed on your machine.

**Note:** It's important to have the correct Spark version set in your environment to avoid conflicts with pre-installed versions.



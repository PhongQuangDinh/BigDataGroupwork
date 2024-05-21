import os
import findspark
findspark.init()
import pyspark
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import lag

def main(inputPath, checkpoint_path, output_path):
    spark = SparkSession.builder.master("local")\
            .appName("Spark Streaming Demonstration")\
            .config("spark.some.config.option", "some-value")\
            .getOrCreate()
    
    spark.conf.set("spark.sql.shuffle.partitions", "2") 

    staticInputDF = (
    spark
        .read    
        .csv(inputPath)
    )
    schema = staticInputDF.schema

    streamingInputDF = (
    spark
        .readStream
        .schema(schema)          
        .csv(inputPath)
    )

    yellowRecordsDF = streamingInputDF.filter(f.col('_c0') == 'yellow')
    yellowRecordsDF = yellowRecordsDF.select(f.col('_c0').alias('Action'),f.col('_c10').alias('dropoff_longitude'), f.col('_c11').alias('dropoff_latitude'), f.col('_c3').alias('dropoff_datetime'))

    greenRecordsDF = streamingInputDF.filter(f.col('_c0') == 'green')
    greenRecordsDF = greenRecordsDF.select(f.col('_c0').alias('Action'),f.col('_c8').alias('dropoff_longitude'), f.col('_c9').alias('dropoff_latitude'), f.col('_c3').alias('dropoff_datetime'))

    goldmanCondition = (
        (col("dropoff_longitude") >= -74.0144185) & (col("dropoff_longitude") <= -74.013777) &
        (col("dropoff_latitude") >= 40.7138745) & (col("dropoff_latitude") <= 40.7152275)
    )

    citigroupCondition = (
        (col("dropoff_longitude") >= -74.012083) & (col("dropoff_longitude") <= -74.009867) &
        (col("dropoff_latitude") >= 40.720053) & (col("dropoff_latitude") <= 40.7217236)
    )

    df = yellowRecordsDF.union(greenRecordsDF)

    goldmanDF = df.filter(goldmanCondition).withColumn("headquarters", lit("goldman"))
    citigroupDF = df.filter(citigroupCondition).withColumn("headquarters", lit("citigroup"))

    filteredDF = goldmanDF.union(citigroupDF)

    streamingCount = (                 
        filteredDF
        .groupBy( 
        filteredDF.Action,
        window(filteredDF.dropoff_datetime, "10 minutes"), filteredDF.headquarters)
        .count()
    )
    def process_batch(batch_df, batch_id):
        batch_df.persist()
        goldman_trends = detect_trends(batch_df, "goldman")
        citigroup_trends = detect_trends(batch_df, "citigroup")
        goldman_trends.show(truncate=False)
        citigroup_trends.show(truncate=False)
        batch_df.unpersist()

    def detect_trends(df, headquarters):
        w = Window.partitionBy("headquarters").orderBy("window")

        df = df.filter(col("headquarters") == headquarters)
        df = df.withColumn("prev_count", lag("count").over(w))

        df = df.filter((col("count") >= 10) & (col("prev_count").isNotNull()))
        df = df.filter(col("count") >= 2 * col("prev_count"))

        trend_df = df.selectExpr(
            "headquarters",
            "count as current_count",
            "window.start as timestamp",
            "prev_count"
        )

        trend_df.write.mode("append").json(output_path)

        return trend_df
    
    query = (
    streamingCount
        .writeStream
        .foreachBatch(process_batch)       
        .outputMode("complete")   
        # .option("checkpointlocation", checkpoint_path)
        .start()
    )
    query.awaitTermination()

    
    
if __name__ == "__main__":
    input_path = sys.argv[sys.argv.index("--input") + 1]
    checkpoint_path = sys.argv[sys.argv.index("--checkpoint") + 1]
    output_path = sys.argv[sys.argv.index("--output") + 1]
    main(input_path, checkpoint_path, output_path)
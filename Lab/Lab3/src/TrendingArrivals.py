import os
import findspark
findspark.init()
import pyspark
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *

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
    def write_to_output(batch_df, batch_id):
        # Collect the current batch
        current_batch = batch_df.collect()

        # Load the previous batch if available
        prev_batch_path = f"{output_path}/previous_batch.parquet"
        try:
            prev_batch_df = spark.read.parquet(prev_batch_path)
            prev_batch = prev_batch_df.collect()
        except:
            prev_batch = []

        # Process the current and previous batches to detect trends
        for current in current_batch:
            current_window_start = int(current['window'].start.timestamp() * 1000)
            current_window_end = int(current['window'].end.timestamp() * 1000)
            current_headquarters = current['headquarters']
            current_count = current['count']

            # Find the corresponding previous interval
            previous_count = 0
            for prev in prev_batch:
                if prev['window'].start == current['window'].start and prev['headquarters'] == current_headquarters:
                    previous_count = prev['count']
                    break

            # Check if the trend detection conditions are met
            if current_count >= 10 and current_count > 2 * previous_count:
                print(f"The number of arrivals to {current_headquarters} has doubled from {previous_count} to {current_count} at {current_window_start}!")

                # Write the current batch to output
                with open(f"{output_path}/part-{current_window_start}.txt", "a") as f:
                    f.write(f"({current_headquarters},({current_count},{current_window_start},{previous_count}))\n")

        # Save the current batch for the next window
        batch_df.write.mode('overwrite').parquet(prev_batch_path)
    query = (
    streamingCount
        .writeStream
        .foreachBatch(write_to_output)       
        .outputMode("complete")   
        # .option("checkpointlocation", checkpoint_path)
        .start()
    )
    query.awaitTermination(60)

    query.stop()
    
if __name__ == "__main__":
    input_path = sys.argv[sys.argv.index("--input") + 1]
    checkpoint_path = sys.argv[sys.argv.index("--checkpoint") + 1]
    output_path = sys.argv[sys.argv.index("--output") + 1]
    main(input_path, checkpoint_path, output_path)
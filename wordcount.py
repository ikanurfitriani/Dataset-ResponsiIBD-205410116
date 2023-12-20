from pyspark import SparkContext
from pyspark.sql import SparkSession
import time

# Create SparkSession and SparkContext
spark = SparkSession.builder\
                    .master("local")\
                    .appName('WordCount')\
                    .getOrCreate()
sc = spark.sparkContext

# Read the input file and Calculating words count
text_file = sc.textFile("Data-25.txt")

# Record start time
start_time = time.time()

counts = text_file.flatMap(lambda line: line.split(" ")) \
                  .map(lambda word: (word, 1)) \
                  .reduceByKey(lambda x, y: x + y)

# Calculate total number of words
total_words = counts.map(lambda x: x[1]).sum()

# Printing each word with its respective count
output = counts.collect()
for (word, count) in output:
    print("%s: %i" % (word, count))

# Record end time
end_time = time.time()

# Print the total number of words
print(f"Total Words: {total_words}")

# Print the time taken
elapsed_time = end_time - start_time
print(f"Time taken: {elapsed_time} seconds")

# Stopping SparkSession and SparkContext
sc.stop()
spark.stop()
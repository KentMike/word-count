import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    # Check if the correct number of command line arguments is provided
    if len(sys.argv) != 3:
        print("Usage: spark-submit count_word.py C:/Users/user/Wordcount-CW/shakes.txt")
        sys.exit(1)

    # create Spark context with Spark configuration
    conf = SparkConf().setAppName("SparkWordCount")
    sc = SparkContext(conf=conf)

    try:
        # get threshold
        threshold = int(sys.argv[2])

        # read in text file and split each document into words
        tokenized = sc.textFile(sys.argv[1]).flatMap(lambda line: line.split(" "))

        # count the occurrence of each word
        wordCounts = tokenized.map(lambda word: (word, 1)).reduceByKey(lambda v1, v2: v1 + v2)

        # filter out words with fewer than threshold occurrences
        filtered = wordCounts.filter(lambda pair: pair[1] >= threshold)

        # count characters
        charCounts = filtered.flatMap(lambda pair: pair[0]).map(lambda c: c).map(lambda c: (c, 1)).reduceByKey(
            lambda v1, v2: v1 + v2
        )

        result_list = charCounts.collect()
        print(repr(result_list)[1:-1])

    finally:
        # Stop the Spark context
        sc.stop()

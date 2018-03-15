'''
Wordcount Spark
Count the words in the complete works of Shakespeare stored
in a file 'words.txt' in HDFS
'''

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("Words count").setMaster("local")
    sc = SparkContext(conf=conf)

    # Create RDD from words.txt file stored in HDFS
    lines = sc.textFile("hdfs:/user/cloudera/words.txt")

    # Transformation 1 : Split every line into words
    # flatMap enables to return multiple values instead of a unique one
    words = lines.flatMap(lambda line: line.split(" "))

    # Transformation 2 : Create tuple for each word
    tuples = words.map(lambda word: (word, 1))

    # Transformation 3 : Group tuples with same key
    counts = tuples.reduceByKey(lambda a, b: (a + b))

    # Note : Transformations could also be concatenated
    # counts = lines.flatMap(lambda line: line.split(" ")) \
    #               .map(lambda word: (word, 1)) \
    #               .reduceByKey(lambda a, b: (a + b))

    # Action 1 : Save counts to HDFS
    # coalesce(1) makes sure all the partitions are grouped and only 1 file is outputed
    counts.coalesce(1).saveAsTextFile('hdfs:/user/cloudera/wordcount/outputDir')

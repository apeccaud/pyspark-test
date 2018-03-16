'''
Spark SQL basic operations and merge
'''

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import *

if __name__ == "__main__":
    conf = SparkConf().setAppName("Words count").setMaster("local")
    sc = SparkContext(conf=conf)

    # Create a new SQLContext from the SparkContext sc
    sqlsc = SQLContext(sc)

    # Create a new Spark DataFrame in the variable df for the Postgres table gameclicks
    df = sqlsc.read.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost/cloudera?user=cloudera") \
        .option("dbtable", "gameclicks") \
        .load()
    
    # Print the schema of the table
    df.printSchema()

    # Get the count of rows
    df.count()

    # Show first 5 rows
    df.show(5)

    # Only select several fields
    df.select("userid", "teamlevel").show(5)

    # Add a WHERE condition
    df.filter(df["teamlevel"] > 1).select("userid", "teamlevel").show(5)

    # GROUP BY condition
    df.groupBy("ishit").count().show()

    # Get the mean and sum
    df.select(mean('ishit'), sum('ishit')).show()

    # Merge table with another table
    df2 = sqlsc.read.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost/cloudera?user=cloudera") \
        .option("dbtable", "adclicks") \
        .load()

    merge = df.join(df2, 'userid')

    # Print schema of merged table
    merge.printSchema()

    # Show first 5 rows
    merge.show(5)
    
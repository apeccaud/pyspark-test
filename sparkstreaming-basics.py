'''
Spark Streaming basic operations
'''

# Example weather station data
#
# 1419408015	0R1,Dn=059D,Dm=066D,Dx=080D,Sn=8.5M,Sm=9.5M,Sx=10.3M
# 1419408016	0R1,Dn=059D,Dm=065D,Dx=078D,Sn=8.5M,Sm=9.5M,Sx=10.3M
# 1419408016	0R2,Ta=13.9C,Ua=28.5P,Pa=889.9H
# 1419408017	0R1,Dn=059D,Dm=064D,Dx=075D,Sn=8.7M,Sm=9.6M,Sx=10.3M
# 1419408018	0R1,Dn=059D,Dm=064D,Dx=075D,Sn=8.9M,Sm=9.6M,Sx=10.3M
# 1419408019	0R1,Dn=059D,Dm=065D,Dx=075D,Sn=8.8M,Sm=9.5M,Sx=10.3M

# Key for measurements:
#
# Sn      Wind speed minimum m/s, km/h, mph, knots #,M, K, S, N
# Sm      Wind speed average m/s, km/h, mph, knots #,M, K, S, N
# Sx      Wind speed maximum m/s, km/h, mph, knots #,M, K, S, N
# Dn      Wind direction minimum deg #, D
# Dm      Wind direction average deg #, D
# Dx      Wind direction maximum deg #, D
# Pa      Air pressure hPa, Pa, bar, mmHg, inHg #, H, P, B, M, I
# Ta      Air temperature °C, °F #, C, F
# Tp      Internal temperature °C, °F #, C, F
# Ua      Relative humidity %RH #, P
# Rc      Rain accumulation mm, in #, M, I
# Rd      Rain duration s #, S
# Ri      Rain intensity mm/h, in/h #, M, I
# Rp      Rain peak intensity mm/h, in/h #, M, I
# Hc      Hail accumulation hits/cm2, hits/in2, hits #, M, I, H
# Hd      Hail duration s #, S
# Hi      Hail intensity hits/cm2h, hits/in2h, hits/ h #, M, I, H
# Hp      Hail peak intensity hits/cm2h, hits/in2h, hits/ h #, M, I, H
# Th      Heating temperature °C, °F #, C, F
# Vh      Heating voltage V #, N, V, W, F2
# Vs      Supply voltage V V
# Vr      3.5 V ref. voltage V V

import re

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("Words count").setMaster("local")
    sc = SparkContext(conf=conf)

    # Create a Spark Streaming context from the Spark Context
    ssc = StreamingContext(sc, 1) # Batch interval of 1s

    # Create a Spark DStream that streams the lines of output from the weather station.
    lines = ssc.socketTextStream("rtd.hpwren.ucsd.edu", 12020)

    # Use flatMap() to iterate over the lines DStream, and calls the parse()
    # function to get the average wind speed.
    vals = lines.flatMap(parse)

    # Create sliding window of data
    # This create a new DStream called window that combines the 10s worth of data and moves by 5s.
    window = vals.window(10, 5)

    # Call the stats() function for each RDD in the DStream window.
    window.foreachRDD(lambda rdd: stats(rdd))

    # Start the stream processing
    ssc.start()

    # After the analysis is done, stop the stream processing
    ssc.stop()

# Parse a line of weather station data, returning the average wind direction measurement 
def parse(line):
    match = re.search("Dm=(\d+)", line)
    if match:
        val = match.group(1)
        return [int(val)]
    return []

def stats(rdd):
    print(rdd.collect())
    if rdd.count() > 0:
        print("max = {}, min = {}".format(rdd.max(), rdd.min()))

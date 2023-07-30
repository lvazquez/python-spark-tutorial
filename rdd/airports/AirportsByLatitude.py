from pyspark import SparkContext, SparkConf

# add current directory to import path
import sys
sys.path.insert(0,'.')
from commons.Utils  import Utils


def get_latitud(line):
    data = Utils.COMMA_DELIMITER.split(line)
    return (data[1], float(data[6]))

    
if __name__ == "__main__":

    '''
    Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
    Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

    Each row of the input file contains the following columns:
    Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
    ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

    Sample output:
    "St Anthony", 51.391944
    "Tofino", 49.082222
    ...
    '''
    
    sparkConf = SparkConf().setAppName("Airports Latitude")\
                           .set("spark.hadoop.validateOutputSpecs", "false")\
                           .setMaster("local[*]")
    sc = SparkContext(conf = sparkConf)
    text_rdd = sc.textFile("in/airports.text")
    # COMMA_DELIMITER is a regexp to split CSV lines
    airport_latitude = text_rdd.map(get_latitud)

    # filter the rows with latitud > 40 and build output
    airports_big = airport_latitude.filter(lambda x: x[1] > 40.0) \
        .map(lambda x: "{}, {}".format(x[0], x[1]))

    # save result to file
    airports_big.saveAsTextFile("out/airports_big_latitud.text")

    # collect & print head results
    for data in airports_big.take(10):
        print(data)


from pyspark import SparkContext, SparkConf

# add current directory to import path
import sys
sys.path.insert(0,'.')
from commons.Utils  import Utils

if __name__ == "__main__":

    '''
    Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
    and output the airport's name and the city's name to out/airports_in_usa.text.

    Each row of the input file contains the following columns:
    Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
    ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

    Sample output:
    "Putnam County Airport", "Greencastle"
    "Dowagiac Municipal Airport", "Dowagiac"
    ...
    '''
    
    sparkConf = SparkConf().setAppName("Airports in USA")\
                           .set("spark.hadoop.validateOutputSpecs", "false")\
                           .setMaster("local[*]")
    sc = SparkContext(conf = sparkConf)

    lines_rdd = sc.textFile("in/airports.text")
    
    # COMMA_DELIMITER is a regexp to split CSV lines
    fields_rdd = lines_rdd.map(lambda line: Utils.COMMA_DELIMITER.split(line)[0:4])
    # filter United States airports
    airports_in_usa = fields_rdd.filter(lambda fields: ('"United States"' == fields[3]))
    # get (name,city) fields
    airports_in_usa_city = airports_in_usa.map(lambda fields: ','.join(fields[1:3]))

    # save result to file
    airports_in_usa_city.saveAsTextFile("out/airports_in_usa.text")

    # collect & print head results
    for data in airports_in_usa_city.take(10):
        print(data)

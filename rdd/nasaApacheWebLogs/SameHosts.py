from pyspark import SparkConf, SparkContext
import re

if __name__ == "__main__":
    
    '''
    "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
    "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
    
    Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
    Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.
    
    Example output:
    vagrant.vf.mmc.com
    www-a1.proxy.aol.com
    .....    
    
    Keep in mind, that the original log files contains the following header lines.
    host    logname    time    method    url    response    bytes
    
    Make sure the head lines are removed in the resulting RDD.
    '''

    def extractHost(line):
        regex = re.compile(r'(\S+)\s')
        m = regex.match(line)
        if m:
            return m.group(1)
        else:
            return None
        
    def notHeader(line):
        return not (line.startswith('host') and 'bytes' in line)
    
    sparkConf = SparkConf().setMaster("local[4]").setAppName("Union Logs")
    sparkConf.set("spark.hadoop.validateOutputSpecs", "false")
    sc = SparkContext.getOrCreate(sparkConf)

    hosts_07 = sc.textFile("in/nasa_19950701.tsv").filter(notHeader).map(extractHost)
    hosts_08 = sc.textFile("in/nasa_19950801.tsv").filter(notHeader).map(extractHost)

    same_hosts = hosts_07.intersection(hosts_08)

    # save to filesystem
    same_hosts.saveAsTextFile("out/nasa_logs_same_hosts.csv")



from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    
    '''
    "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
    "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
    Create a Spark program to generate a new RDD which contains the log lines from both July 1st and August 1st,
    take a 0.1 sample of those log lines and save it to "out/sample_nasa_logs.tsv" file.
    
    Keep in mind, that the original log files contains the following header lines.
    host    logname    time    method    url    response    bytes
    
    Make sure the head lines are removed in the resulting RDD.
    '''
    
    def not_header(line):
        return not (line.startswith('host') and 'bytes' in line)
    
    sparkConf = SparkConf().setMaster("local[4]").setAppName("Union Logs")
    sparkConf.set("spark.hadoop.validateOutputSpecs", "false")
    sc = SparkContext.getOrCreate(sparkConf)

    logs_07 = sc.textFile("in/nasa_19950701.tsv").filter(not_header)
    logs_08 = sc.textFile("in/nasa_19950801.tsv").filter(not_header)

    nasa_logs = logs_07.union(logs_08)

    sample_logs = nasa_logs.sample(withReplacement=False, fraction=0.1, seed=17)

    # save to filesystem
    sample_logs.saveAsTextFile("out/sample_nasa_logs.tsv")
    

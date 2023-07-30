from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("word count").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    
    lines = sc.textFile("in/word_count.text")
    
    # Transforms
    words = lines.flatMap(lambda line: line.split(" "))
    wordPairs = words.map(lambda x: (x, 1))
    wordCounts = wordPairs.groupByKey().mapValues(len)

    # Action
    topWords = sorted(wordCounts.collect(), key=lambda k_v: k_v[1], reverse=True)[0:10]
    for word, count in topWords:
        print("{} : {}".format(word, count))

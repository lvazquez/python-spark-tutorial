import sys
sys.path.insert(0, '.')
from pyspark import SparkContext, SparkConf
from commons.Utils import Utils

def loadPostCodeMap():
    lines = open("in/uk-postcode.csv", "r").read().split("\n")
    splitsForLines = [Utils.COMMA_DELIMITER.split(line) for line in lines if line != ""]
    return {splits[0]: splits[7] for splits in splitsForLines}

def getPostPrefix(line: str):
    postcode = Utils.COMMA_DELIMITER.split(line)[4]
    postPrefix = postcode.split(" ")[0]
    return None if not postPrefix else postPrefix

if __name__ == "__main__":
    conf = SparkConf().setAppName('UkMakerSpaces').setMaster("local[*]")
    sc = SparkContext(conf = conf)

    postCodeMap = sc.broadcast(loadPostCodeMap())

    makerSpaceRdd = sc.textFile("in/uk-makerspaces-identifiable-data.csv")\
                      .filter(lambda line: not line.startswith("Timestamp"))

    makerCodes = makerSpaceRdd.map(getPostPrefix)\
                                  .filter(lambda x:  x is not None) \
                                  
    regions = makerCodes.map(lambda code: postCodeMap.value.get(code, "Unknow"))

    for region, count in regions.countByValue().items():
        print("{} : {}".format(region, count))

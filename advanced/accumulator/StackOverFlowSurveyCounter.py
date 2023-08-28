import sys
sys.path.insert(0, '.')
from pyspark import SparkContext, SparkConf
from commons.Utils import Utils

if __name__ == "__main__":
    conf = SparkConf().setAppName('StackOverFlowSurvey').setMaster("local[*]")
    sc = SparkContext(conf = conf)
    total = sc.accumulator(0)
    totalBytes = sc.accumulator(0)
    missingSalaryMidPoint = sc.accumulator(0)
    def getCountrySalaryAndCount(response):
        # increment total counter
        fields = Utils.COMMA_DELIMITER.split(response)
        total.add(1)
        totalBytes.add(len(response.encode('utf-8')))
        if not fields[14]:
            missingSalaryMidPoint.add(1)
        # return (country, salary) pairs 
        return (fields[2], fields[14])

    # load and skip head
    surveyResponsesRDD = sc.textFile("in/2016-stack-overflow-survey-responses.csv").filter(lambda x: not x.startswith('collector'))

    countrySalaryRdd = surveyResponsesRDD.map(getCountrySalaryAndCount)
    canadaSalary = countrySalaryRdd.filter(lambda x: x[0] == "Canada")

    print("Count of responses from Canada: {}".format(canadaSalary.count()))
    print("Total count of responses: {}".format(total.value))
    print("Total bytes processed: {}".format(totalBytes.value))
    print("Count of responses missing salary middle point: {}" \
        .format(missingSalaryMidPoint.value))

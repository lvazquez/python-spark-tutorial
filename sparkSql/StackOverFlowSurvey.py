from pyspark.sql import SparkSession
from pyspark.sql.functions import col,avg
import sys

if __name__ == "__main__":

    spark = SparkSession.builder.appName("StackOverFlowSurvey").master("local[*]").getOrCreate()

    dataFrameReader = spark.read

    responses = dataFrameReader \
        .option("header", "true") \
        .option("inferSchema", value = True) \
        .csv("in/2016-stack-overflow-survey-responses.csv")

    responseSelected = responses.select("country", "occupation", 
        "age_midpoint", "salary_midpoint")

    print("=== Print out schema ===")
    responseSelected.printSchema()
    
    print("=== Filter rows with NULL occupation ===")
    responseSelected = responseSelected.filter(col("occupation").isNotNull())
    
    print("=== Print the selected columns of the table ===")
    responseSelected.show()
    
    print("=== Print records where the response is from Afghanistan ===")
    responseSelected.filter(col('country') == "Afghanistan").show()

    print("=== Print the count of occupations ===")
    groupedData = responseSelected.groupBy("occupation")
    groupedData.count().show()

    print("=== Print records with average mid age less than 20 ===")
    responseSelected.filter(col("age_midpoint") < 20).show()

    print("=== Print the result by salary middle point in descending order ===")
    responseSelected.orderBy(col("salary_midpoint"), ascending = False).show()

    print("=== Group by country and aggregate by average salary middle point ===")
    dataGroupByCountry = responseSelected.groupBy("country")
    dataGroupByCountry.agg(avg("salary_midpoint").alias("average_salary"))\
                      .orderBy(col("average_salary"), ascending = False).show()

    responseSalaryBucket = responseSelected.filter(col("salary_midpoint").isNotNull())\
                                           .withColumn(
                                               "salary_bucket",
                                               (col("salary_midpoint")/20000).cast("integer")*20000
                                           )

    print("=== With salary bucket column ===")
    responseSalaryBucket.select("salary_midpoint", "salary_bucket").show()

    print("=== Group by salary bucket ===")
    responseSalaryBucket.groupBy("salary_bucket")\
                        .count().orderBy("salary_bucket").show()

    spark.stop()

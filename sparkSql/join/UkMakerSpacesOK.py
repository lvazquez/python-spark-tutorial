from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import sys

if __name__ == "__main__":
    session = SparkSession.builder.appName("UkMakerSpaces").master("local[*]").getOrCreate()

    makerSpace = session.read\
                        .option("header", "true") \
                        .csv("in/uk-makerspaces-identifiable-data.csv")\
                        .select(f.col("Name of makerspace").alias("Name"), "Postcode")
    
    
    ## select postcode prefix
    makerSpacePost = makerSpace.withColumn("Code",
                                           f.regexp_replace(f.trim("Postcode"), "\s.*", ""))

    print("=== Print 20 records of makerspace table ===")
    makerSpacePost.show(10)

    postCode = session.read\
                      .option("header", "true")\
                      .csv("in/uk-postcode.csv")\
                      .select(f.trim("Postcode").alias("Code"), "Region")
    
    print("=== Print 20 records of postcode table ===")
    postCode.show(10)

    
    joined = makerSpacePost.join(postCode,
                                 makerSpacePost["Code"] == postCode["Code"],
                                 "left_outer")

    print("=== Group by Region ===")
    joined.groupBy("Region").count().orderBy("count", ascending = False).show(40)

from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as func
import sys

if __name__=='__main__':
    input_file = sys.argv[1] 
    output_folder = sys.argv[2]

    sc = SparkContext()
    sql_c = SQLContext(sc)

    #df1 = sql_c.read.csv("Parking_Violations_Issued_-_Fiscal_Year_2016.csv", header=True)
    df1 = sql_c.read.csv("Book1.csv", header=True)
    df2 = sql_c.read.csv("Centerline.csv", header=True)

    def city(cName):
    if cName == "MAN" or cName == "MH" or cName == "MN" or cName == "NEWY" or cName == "NEW Y" or cName == "NY":
        return "1"
    elif cName == "BX" or cName == "BRONX":
        return "2"
    elif cName == "BK" or cName == "K" or cName == "KING" or cName == "KINGS":
        return "3"
    elif cName == "Q" or cName == "QN" or cName == "QNS" or cName == "QU" or cName == "QUEEN":
        return "4"
    elif cName == "R" or cName == "RICHMOND":
        return "5"
    else: return "*!NULL!*"

    spark.udf.register("city", city)


    city = func.udf(city)

    df1 = df1.select(
    func.col("House Number").cast("int").alias("House Number"), 
    func.upper(func.col("Street Name")).alias("Street Name"), 
    city(func.col("Violation County")).cast("int").alias("BOROCODE")
    )

    df2 = df2.select(
        func.col("PHYSICALID").cast("int").alias("PHYSICALID"),
        func.upper(func.col("FULL_STREE")).alias("FULL_STREE"),
        func.upper(func.col("ST_LABEL")).alias("ST_LABEL"),
        func.col("BOROCODE").cast("int").alias("BOROCODE"),
        func.col("L_LOW_HN").cast("int").alias("L_LOW_HN"),
        func.col("L_HIGH_HN").cast("int").alias("L_HIGH_HN"),
        func.col("R_LOW_HN").cast("int").alias("R_LOW_HN"),
        func.col("R_HIGH_HN").cast("int").alias("R_HIGH_HN")
    )

    df1.join(
    df2, (df1["BOROCODE"] == df2["BOROCODE"]) & #join 1  #join2 \/
    ((df1["Street Name"] == df2["ST_LABEL"]) | (df1["Street Name"] == df2["FULL_STREE"]))).filter(
        (
            (func.col("House Number")%2 != 0) &
            (func.col("House Number") >= func.col("L_LOW_HN")) &
            (func.col("House Number") <= func.col("L_HIGH_HN"))
        ) | (
            (func.col("House Number")%2 == 0) &
            (func.col("House Number") >= func.col("R_LOW_HN")) &
            (func.col("House Number") <= func.col("R_HIGH_HN"))
        )).count()

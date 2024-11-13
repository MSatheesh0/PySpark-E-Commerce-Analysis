from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,DoubleType,DateType
from pyspark.sql import functions as F
from datetime import date
from pyspark.sql.window import Window

# Initialize Spark session
a=SparkSession.builder.appName("E-commerce Transactions Analysis").getOrCreate()

# Define the schema for the DataFrame
b=StructType([
    StructField("a",StringType(),True),
    StructField("b",StringType(),True),
    StructField("c",StringType(),True),
    StructField("d",StringType(),True),
    StructField("e",DoubleType(),True),
    StructField("f",DateType(),True)
])

# Create sample data
g=[
    ("T1","U1","P1","Music",15.0,date(2024,1,15)),
    ("T2","U2","P2","Music",25.0,date(2024,1,20)),
    ("T3","U3","P3","Home Appliances",80.0,date(2024,2,1)),
    ("T4","U4","P4","Home Appliances",150.0,date(2024,2,3)),
    ("T5","U5","P5","Health",45.0,date(2024,3,7)),
    ("T6","U6","P6","Health",60.0,date(2024,3,9)),
    ("T7","U7","P7","Gaming",120.0,date(2024,4,1)),
    ("T8","U8","P8","Gaming",180.0,date(2024,4,5)),
    ("T9","U9","P9","Kitchen",55.0,date(2024,5,3)),
    ("T10","U10","P10","Kitchen",70.0,date(2024,5,7)),
]

# Create DataFrame
h=a.createDataFrame(g,b)
h.show()

# Calculate total spending amount and average transaction amount per user
i=h.groupBy("b").agg(
    F.sum("e").alias("j"),
    F.avg("e").alias("k")
)
i.show()

# Calculate the most frequently purchased category for each user
l=h.groupBy("b","d")\
    .count()\
    .withColumn("m",F.row_number().over(
        Window.partitionBy("b").orderBy(F.desc("count"))))\
    .filter(F.col("m")==1)\
    .select("b",F.col("d").alias("n"))
h.show()

# Join results
o=i.join(l,on="b",how="left")

# Show the result
o.show()

import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")


print("######################################")
print(sc.version)
print("######################################")

input_1 = sys.argv[1]
input_2 = sys.argv[2]
input_3 = sys.argv[3]
input_4 = sys.argv[4]
output = sys.argv[5]

sales_sh = (
    StructType()
    .add("sssolddatesk", IntegerType(), True)
    .add("sssoldtimesk", IntegerType(), True)
    .add("ssitemsk", IntegerType(), True)
    .add("sscustomersk", IntegerType(), True)
    .add("sscdemosk", IntegerType(), True)
    .add("sshdemosk", IntegerType(), True)
    .add("ssaddrsk", IntegerType(), True)
    .add("ssstoresk", IntegerType(), True)
    .add("sspromosk", IntegerType(), True)
    .add("ssticketnumber", IntegerType(), True)
    .add("ssquantity", IntegerType(), True)
    .add("sswholesalecost", DoubleType(), True)
    .add("sslistprice", DoubleType(), True)
    .add("sssalesprice", DoubleType(), True)
    .add("ssextdiscountamt", DoubleType(), True)
    .add("ssextsalesprice", DoubleType(), True)
    .add("ssextwholesalecost", DoubleType(), True)
    .add("ssextlistprice", DoubleType(), True)
    .add("ssexttax", DoubleType(), True)
    .add("sscouponamt", DoubleType(), True)
    .add("ssnetpaid", DoubleType(), True)
    .add("ssnetpaidinctax", DoubleType(), True)
    .add("ssnetprofit", DoubleType(), True)
)


store_sh = (
    StructType()
    .add("sstoresk", IntegerType(), True)
    .add("sstoreid", StringType(), True)
    .add("srecstartdate", StringType(), True)
    .add("srecenddate", StringType(), True)
    .add("scloseddatesk", IntegerType(), True)
    .add("sstorename", StringType(), True)
    .add("snumberemployees", IntegerType(), True)
    .add("sfloorspace", IntegerType(), True)
    .add("shours", StringType(), True)
    .add("smanager", StringType(), True)
    .add("smarketid", IntegerType(), True)
    .add("sgeographyclass", StringType(), True)
    .add("smarketdesc", StringType(), True)
    .add("smarketmanager", StringType(), True)
    .add("sdivisionid", IntegerType(), True)
    .add("sdivisionname", StringType(), True)
    .add("scompanyid", IntegerType(), True)
    .add("scompanyname", StringType(), True)
    .add("sstreetnumber", StringType(), True)
    .add("sstreetname", StringType(), True)
    .add("sstreettype", StringType(), True)
    .add("ssuitenumber", StringType(), True)
    .add("scity", StringType(), True)
    .add("scounty", StringType(), True)
    .add("sstate", StringType(), True)
    .add("szip", StringType(), True)
    .add("scountry", StringType(), True)
    .add("sgmtoffset", DoubleType(), True)
    .add("staxprecentage", DoubleType(), True)
)


date_sh = (
    StructType()
    .add("ddatesk", IntegerType(), True)
    .add("ddateid", StringType(), True)
    .add("ddate", StringType(), True)
    .add("dmonthseq", IntegerType(), True)
    .add("dweekseq", IntegerType(), True)
    .add("dquarterseq", IntegerType(), True)
    .add("dyear", IntegerType(), True)
    .add("ddow", IntegerType(), True)
    .add("dmoy", IntegerType(), True)
    .add("ddom", IntegerType(), True)
    .add("dqoy", IntegerType(), True)
    .add("dfyyear", IntegerType(), True)
    .add("dfyquarterseq", IntegerType(), True)
    .add("dfyweekseq", IntegerType(), True)
    .add("ddayname", StringType(), True)
    .add("dquartername", StringType(), True)
    .add("dholiday", StringType(), True)
    .add("dweekend", StringType(), True)
    .add("dfollowingholiday", StringType(), True)
    .add("dfirstdom", IntegerType(), True)
    .add("dlastdom", IntegerType(), True)
    .add("dsamedayly", IntegerType(), True)
    .add("dsamedaylq", IntegerType(), True)
    .add("dcurrentday", StringType(), True)
    .add("dcurrentweek", StringType(), True)
    .add("dcurrentmonth", StringType(), True)
    .add("dcurrentquarter", StringType(), True)
    .add("dcurrentyear", StringType(), True)
)


item_sh = (
    StructType()
    .add("iitemsk", IntegerType(), True)
    .add("iitemid", StringType(), True)
    .add("irecstartdate", StringType(), True)
    .add("irecenddate", StringType(), True)
    .add("iitemdesc", StringType(), True)
    .add("icurrentprice", DoubleType(), True)
    .add("iwholesalecost", DoubleType(), True)
    .add("ibrandid", IntegerType(), True)
    .add("ibrand", StringType(), True)
    .add("iclassid", IntegerType(), True)
    .add("iclass", StringType(), True)
    .add("icategoryid", IntegerType(), True)
    .add("icategory", StringType(), True)
    .add("imanufactid", IntegerType(), True)
    .add("imanufact", StringType(), True)
    .add("isize", StringType(), True)
    .add("iformulation", StringType(), True)
    .add("icolor", StringType(), True)
    .add("iunits", StringType(), True)
    .add("icontainer", StringType(), True)
    .add("imanagerid", IntegerType(), True)
    .add("iproductname", StringType(), True)
)


sales = spark.read.option("delimiter", "|").schema(sales_sh).csv(input_1)
store = spark.read.option("delimiter", "|").schema(store_sh).csv(input_2)
date = spark.read.option("delimiter", "|").schema(date_sh).csv(input_3)
item = spark.read.option("delimiter", "|").schema(item_sh).csv(input_4)

sales.createOrReplaceTempView("sales")
store.createOrReplaceTempView("store")
date.createOrReplaceTempView("date")
item.createOrReplaceTempView("item")

result = spark.sql(
    """SELECT s.sstorename AS Store,
               ss.ssquantity AS quantity,
               ss.ssnetpaid AS Revenue,
               d.dyear AS year,
               i.ibrand AS brand
     FROM sales AS ss
     LEFT JOIN store AS s
     ON ss.ssstoresk = s.sstoresk
     LEFT JOIN date AS d
     ON ss.sssolddatesk = d.ddatesk
     LEFT JOIN item AS i
     ON ss.ssitemsk = i.iitemsk"""
)


result.coalesce(1).write.option("header", True).csv(output)


print("######################################")
print("Transformation COMPLETED")
print("######################################")

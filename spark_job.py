from pyspark.sql.session import SparkSession
import pandas as pd
from irsx.xmlrunner import XMLRunner
from pyspark.sql.types import StringType, StructType, StructField, IntegerType
from pyspark.sql.functions import udf

path = "/opt/conda/miniconda3/lib/python3.8/site-packages/irsx/CSV/index_2021.csv"

df21 = pd.read_csv(path, index_col=False, dtype=str, nrows= 50) # read all as string, not beautiful but we only need object id anyways
df21.head()
spark = SparkSession.builder.getOrCreate()
sdf = spark.createDataFrame(df21["OBJECT_ID"], StringType())

xml_runner = XMLRunner()
def transform_data(e):
    try:
        filing = xml_runner.run_filing(e)
        schedules = filing.list_schedules()
    except:
        print(f"Transform error for id {e}")
        return ["","","","",0, 0, 0, 0, 0, 0,0, 0]
    
    ein = 0
    state = 0
    name = 0
    revenue = 0
    revenueEZ = 0
    vol_cnt, empl_cnt, rvn_ls_exp, liab_eoy, liab_boy,assts_eoy, assts_boy = 0, 0, 0, 0, 0,0, 0
    
    if "ReturnHeader990x" in schedules:
        header = filing.get_parsed_sked("ReturnHeader990x")
        header_part_i = header[0]["schedule_parts"]["returnheader990x_part_i"]
        ein = header_part_i["ein"]
        state = header_part_i.get("USAddrss_SttAbbrvtnCd", "XX")
        name = header_part_i["BsnssNm_BsnssNmLn1Txt"]
        
    if "IRS990EZ" in schedules:
        irs990ez = filing.get_parsed_sked("IRS990EZ")
        irs990ez_part_i = irs990ez[0]["schedule_parts"].get("ez_part_i", None)
        if irs990ez_part_i:
            revenueEZ = irs990ez_part_i.get("TtlRvnAmt", 0)        
    
    if "IRS990" in schedules:
        irs990 = filing.get_parsed_sked("IRS990")
        irs990_part_i = irs990[0]["schedule_parts"]["part_i"]
        revenue = irs990_part_i["CYTtlRvnAmt"]
        vol_cnt = int(irs990_part_i.get("TtlVlntrsCnt", 0))
        empl_cnt = int(irs990_part_i.get("TtlEmplyCnt", 0))
        rvn_ls_exp = int(irs990_part_i.get("CYRvnsLssExpnssAmt", 0))
        liab_eoy = int(irs990_part_i.get("TtlLbltsEOYAmt", 0))
        liab_boy = int(irs990_part_i.get("TtlLbltsBOYAmt", 0))
        assts_eoy = int(irs990_part_i.get("TtlAsstsEOYAmt", 0))
        assts_boy = int(irs990_part_i.get("TtlAsstsBOYAmt", 0))



    
    revenue = int(revenue) + int(revenueEZ)
    return [e, ein, state, name, revenue, vol_cnt, empl_cnt, rvn_ls_exp, liab_eoy, liab_boy,assts_eoy, assts_boy ]
     
    
my_schema = StructType([
    StructField("ObjectID", StringType(), nullable=False),
    StructField("EIN", StringType(), nullable=False),
    StructField("State", StringType(), nullable=False),
    StructField("Name", StringType(), nullable=False),
    StructField("Revenue", IntegerType(), nullable=False),
    StructField("TtlVlntrsCnt", IntegerType(), nullable=False),
    StructField("TtlEmplyCnt", IntegerType(), nullable=False),
    StructField("CYRvnsLssExpnssAmt", IntegerType(), nullable=False),
    StructField("TtlLbltsEOYAmt", IntegerType(), nullable=False),
    StructField("TtlLbltsBOYAmt", IntegerType(), nullable=False),
    StructField("TtlAsstsEOYAmt", IntegerType(), nullable=False),
    StructField("TtlAsstsBOYAmt", IntegerType(), nullable=False),
])

spark_transform_data = udf(lambda z: transform_data(z), my_schema)
spark.udf.register("spark_transform_data", spark_transform_data)


# full df
anz = sdf.count()
print(anz)
sdf2 = sdf.withColumn('valuelist', spark_transform_data('value')).select("valuelist.*")
sdf2.explain()
pdf = sdf2.toPandas()
pdf.to_csv(f"hdfs://big-spark-cluster-m/user/root/{anz}.csv", index=False)
pdf.to_parquet(f"hdfs://big-spark-cluster-m/user/root/{anz}.parquet", index=False)

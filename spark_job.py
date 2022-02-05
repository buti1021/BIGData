from pyspark.sql.session import SparkSession
import pandas as pd
from pyspark.sql.types import StringType

path = "/opt/conda/miniconda3/lib/python3.8/site-packages/irsx/CSV/index_2021.csv"

df21 = pd.read_csv(path, index_col=False, dtype=str, nrows= 50) # read all as string, not beautiful but we only need object id anyways
df21.head()
spark = SparkSession.builder.getOrCreate()
sdf = spark.createDataFrame(df21["OBJECT_ID"], StringType())
sdf.explain()
anz = sdf.count()
sdf.toPandas().to_csv(f"file:///BIGData/{anz}.csv", index=None)
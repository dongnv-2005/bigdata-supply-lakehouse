import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, round as spark_round, when

start_time = time.time()

# 1. Khoi tao SparkSession toi uu cho single node
spark = SparkSession.builder \
    .appName("Cainiao_LaDe_Bronze_to_Silver_ETL") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.sql.files.maxRecordsPerFile", "500000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 65)
print(">>> [1/4] DOC DU LIEU XUAT HANG (DELIVERY) TU HDFS BRONZE...")
print("=" * 65)

delivery_bronze_path = "hdfs://localhost:9000/data/lade/delivery/*.csv"
df_delivery_raw = spark.read.option("header", "true") \
                            .option("inferSchema", "true") \
                            .csv(delivery_bronze_path)

delivery_count = df_delivery_raw.count()
print(f">>> Da doc thanh cong {delivery_count} ban ghi Delivery!")

print(">>> [2/4] CHUAN HOA SCHEMA, TINH THOI GIAN GIAO HANG VA NHAN RUI RO...")
df_delivery_clean = df_delivery_raw \
    .withColumn("accept_ts", to_timestamp(col("accept_time"), "MM-dd HH:mm:ss")) \
    .withColumn("delivery_ts", to_timestamp(col("delivery_time"), "MM-dd HH:mm:ss")) \
    .withColumn("delivery_duration_min", 
                spark_round((col("delivery_ts").cast("long") - col("accept_ts").cast("long")) / 60, 2)) \
    .withColumn("late_delivery_risk", 
                when(col("delivery_duration_min") > 180, 1).otherwise(0)) \
    .filter(col("city").isNotNull() & col("delivery_duration_min").isNotNull() & (col("delivery_duration_min") >= 0))

print(">>> Mau du lieu Delivery sau khi xu ly:")
df_delivery_clean.select("order_id", "city", "courier_id", "delivery_duration_min", "late_delivery_risk").show(5)

silver_delivery_path = "hdfs://localhost:9000/silver/lade/delivery_parquet"
print(f">>> [3/4] GHI PARQUET NEN SNAPPY PHAN VUNG THEO CITY TAI: {silver_delivery_path}...")
df_delivery_clean.repartition("city").write \
    .mode("overwrite") \
    .partitionBy("city") \
    .parquet(silver_delivery_path)

print("=" * 65)
print(">>> [4/4] XU LY DU LIEU NHAP HANG (PICKUP) TU HDFS BRONZE...")
print("=" * 65)

pickup_bronze_path = "hdfs://localhost:9000/data/lade/pickup/*.csv"
df_pickup_raw = spark.read.option("header", "true") \
                          .option("inferSchema", "true") \
                          .csv(pickup_bronze_path)

pickup_count = df_pickup_raw.count()
print(f">>> Da doc thanh cong {pickup_count} ban ghi Pickup!")

df_pickup_clean = df_pickup_raw \
    .withColumn("accept_ts", to_timestamp(col("accept_time"), "MM-dd HH:mm:ss")) \
    .withColumn("pickup_ts", to_timestamp(col("pickup_time"), "MM-dd HH:mm:ss")) \
    .withColumn("pickup_duration_min", 
                spark_round((col("pickup_ts").cast("long") - col("accept_ts").cast("long")) / 60, 2)) \
    .filter(col("city").isNotNull())

silver_pickup_path = "hdfs://localhost:9000/silver/lade/pickup_parquet"
print(f">>> GHI PARQUET NEN SNAPPY PHAN VUNG THEO CITY TAI: {silver_pickup_path}...")
df_pickup_clean.repartition("city").write \
    .mode("overwrite") \
    .partitionBy("city") \
    .parquet(silver_pickup_path)

elapsed_sec = round(time.time() - start_time, 2)
print("=" * 65)
print(f">>> GIAI DOAN 2 HOAN TAT TRONG {elapsed_sec} GIAY!")
print(f">>> Tong so ban ghi da xu ly: {delivery_count + pickup_count}")
print("=" * 65)

spark.stop()

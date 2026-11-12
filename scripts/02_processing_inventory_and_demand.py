import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, count, sum as spark_sum, avg, round as spark_round, coalesce, lit
from pyspark.sql.window import Window

# 1. Khoi tao SparkSession
spark = SparkSession.builder \
    .appName("Cainiao_LaDe_Processing_and_Benchmark") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 75)
print(">>> [BÀI TOÁN 1] TÍNH TOÁN XUẤT - NHẬP - TỒN LŨY KẾ BẰNG SPARK WINDOWING...")
print("=" * 75)

# Đọc dữ liệu từ tầng Silver Parquet
df_delivery = spark.read.parquet("hdfs://localhost:9000/silver/lade/delivery_parquet")
df_pickup = spark.read.parquet("hdfs://localhost:9000/silver/lade/pickup_parquet")

# Tổng hợp số lượng Xuất (Delivery) theo từng trạm/hub (aoi_id) và ngày (ds)
df_outbound = df_delivery.groupBy("city", "aoi_id", "ds") \
                         .agg(count("order_id").alias("outbound_packages"))

# Tổng hợp số lượng Nhập (Pickup) theo từng trạm/hub (aoi_id) và ngày (ds)
df_inbound = df_pickup.groupBy("city", "aoi_id", "ds") \
                      .agg(count("order_id").alias("inbound_packages"))

# Ghép 2 luồng Nhập và Xuất
df_flow = df_inbound.join(df_outbound, on=["city", "aoi_id", "ds"], how="full_outer") \
                    .na.fill(0) \
                    .withColumn("daily_net_change", col("inbound_packages") - col("outbound_packages"))

# Ứng dụng Spark Windowing: Tính số dư tồn kho tích lũy theo thời gian trên RAM
window_spec = Window.partitionBy("city", "aoi_id").orderBy("ds").rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_inventory = df_flow.withColumn("cumulative_inventory", spark_sum("daily_net_change").over(window_spec))

print(">>> [KẾT QUẢ 1] Bảng theo dõi Xuất - Nhập - Tồn lũy kế tại các Trạm/Hub mẫu:")
df_inventory.filter(col("aoi_id") > 0) \
            .orderBy("city", "aoi_id", "ds") \
            .select("city", "aoi_id", "ds", "inbound_packages", "outbound_packages", "daily_net_change", "cumulative_inventory") \
            .show(10, truncate=False)

# Lưu kết quả bài toán 1 sang HDFS
df_inventory.write.mode("overwrite").parquet("hdfs://localhost:9000/silver/lade/inventory_summary_parquet")

print("=" * 75)
print(">>> [BÀI TOÁN 2] PHÂN TÍCH NHU CẦU & RỦI RO GIAO TRỄ THEO KHUNG GIỜ (OLAP)...")
print("=" * 75)

# Trích xuất khung giờ từ accept_ts và phân tích tỷ lệ trễ
df_hourly = df_delivery.withColumn("accept_hour", hour(col("accept_ts"))) \
                       .groupBy("city", "accept_hour") \
                       .agg(
                           count("order_id").alias("total_orders"),
                           spark_sum("late_delivery_risk").alias("late_orders"),
                           spark_round(avg("delivery_duration_min"), 1).alias("avg_duration_min"),
                           spark_round((spark_sum("late_delivery_risk") / count("order_id")) * 100, 2).alias("late_rate_pct")
                       ) \
                       .orderBy("city", "accept_hour")

print(">>> [KẾT QUẢ 2] Bảng phân tích nhu cầu và tỷ lệ giao trễ theo khung giờ:")
df_hourly.show(15)

df_hourly.write.mode("overwrite").parquet("hdfs://localhost:9000/silver/lade/hourly_demand_parquet")

print("=" * 75)
print(">>> [BENCHMARK 2] SO SÁNH HIỆU NĂNG: PARTITION PRUNING VS FULL TABLE SCAN...")
print("=" * 75)

# TEST 1: Quét có phân vùng (Partition Pruning) trên Silver Parquet
# Chỉ truy vấn thành phố Shanghai -> Spark chỉ quét thư mục city=Shanghai
t0 = time.time()
count_shanghai_silver = spark.read.parquet("hdfs://localhost:9000/silver/lade/delivery_parquet") \
                             .filter(col("city") == "Shanghai") \
                             .count()
t1 = time.time()
time_silver_pruning = round(t1 - t0, 3)

# TEST 2: Quét toàn bộ (Full Table Scan) trên file CSV thô Bronze
# Spark phải quét toàn bộ 5 file CSV của 5 thành phố để lọc lấy Shanghai
t2 = time.time()
count_shanghai_bronze = spark.read.option("header", "true").csv("hdfs://localhost:9000/data/lade/delivery/*.csv") \
                             .filter(col("city") == "Shanghai") \
                             .count()
t3 = time.time()
time_bronze_scan = round(t3 - t2, 3)

speedup = round(time_bronze_scan / time_silver_pruning, 2) if time_silver_pruning > 0 else 0

benchmark_report = f"""
========================================================================================
                      BÁO CÁO BENCHMARK HIỆU NĂNG TRUY VẤN
========================================================================================
Truy vấn kiểm thử: Đếm tổng số đơn hàng giao tại Thượng Hải (Shanghai)
- Số lượng bản ghi kết quả: {count_shanghai_silver:,} dòng

1. Quét trên Bronze Layer (Full Table Scan CSV thô):
   - Cơ chế: Spark phải duyệt qua toàn bộ 10.6M bản ghi trên tất cả các file CSV.
   - Thời gian thực thi: {time_bronze_scan} giây

2. Quét trên Silver Layer (Partition Pruning Parquet):
   - Cơ chế: Spark chỉ nhảy vào đúng thư mục city=Shanghai, bỏ qua 4 thành phố còn lại.
   - Thời gian thực thi: {time_silver_pruning} giây

=> KẾT LUẬN HIỆU NĂNG:
   Kỹ thuật Partition Pruning kết hợp Parquet giúp tăng tốc độ truy vấn gấp {speedup} LẦN!
========================================================================================
"""
print(benchmark_report)

# Ghi báo cáo ra file
with open("/home/dongnv/bigdata_project/benchmark/benchmark_query_performance.txt", "w", encoding="utf-8") as f:
    f.write(benchmark_report)

print(">>> Đã lưu kết quả đo đạc tại: ~/bigdata_project/benchmark/benchmark_query_performance.txt")

spark.stop()

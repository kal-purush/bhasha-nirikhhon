import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv


# .env 파일 로드
load_dotenv()

spark = (
    SparkSession.builder.appName("MinIO-Spark")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.262",
    )
    .getOrCreate()
)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

# MinIO 접근 설정
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
hadoop_conf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
hadoop_conf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")


# 설정값 다시 확인
print("fs.s3a.impl:", hadoop_conf.get("fs.s3a.impl"))
print("fs.s3a.endpoint:", hadoop_conf.get("fs.s3a.endpoint"))
print("fs.defaultFS:", hadoop_conf.get("fs.defaultFS"))


# 데이터 읽기
df = spark.read.csv(
    "s3a://job-data/jasoseol_20250220.csv", header=True, inferSchema=True
)

df.show()

spark.stop()
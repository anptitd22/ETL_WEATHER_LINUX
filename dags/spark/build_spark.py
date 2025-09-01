from dotenv import load_dotenv
from pyspark import SparkConf
from pyspark.sql import SparkSession
import os

path_env = "../.env"

load_dotenv(path_env)

SQL_TEST_USER = os.getenv("SQL_TEST_USER")
SQL_TEST_PASS = os.getenv("SQL_TEST_PASS")
SQL_TEST_SERVICE = os.getenv("SQL_TEST_SERVICE")
SQL_TEST_HOST = os.getenv("SQL_TEST_HOST")
SQL_TEST_PORT = os.getenv("SQL_TEST_PORT")
SQL_TEST_ORACLE_DOCKER = os.getenv("SQL_TEST_ORACLE_DOCKER")


def get_spark():
    # JAR mới cần thêm (KHÔNG xoá JAR cũ)
    iceberg_runtime = "/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar"
    iceberg_aws_bundle = "/opt/spark/jars/iceberg-aws-bundle-1.9.2.jar"
    iceberg_nessie = "/opt/spark/jars/iceberg-nessie-1.9.2.jar"
    all_opt = '/opt/spark/jars/*'

    # build lại chuỗi spark.jars: giữ cái cũ + thêm Iceberg
    existing_jars = "/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.787.jar,/opt/spark/jars/ojdbc11.jar"
    all_jars = f"{existing_jars},{iceberg_runtime},{iceberg_aws_bundle}"

    spark = ( SparkSession.builder
        .appName("BAITEST")
        .master("local[*]")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
          .config("spark.sql.catalog.local.type", "nessie")
          .config("spark.sql.catalog.local.uri", "http://nessie:19120/api/v2")
        .config("spark.sql.catalog.local.warehouse", "s3a://warehouse/")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "admin12345")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.jars", all_jars)
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
        .config("spark.hadoop.fs.s3a.connection.request.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.part.upload.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.idle.time", "60000")
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000")
        .config("spark.driver.extraClassPath", all_opt)
        .config("spark.executor.extraClassPath", all_opt)
        .config("spark.hadoop.fs.s3a.vectored.read.min.seek.size", "131072")
        .config("spark.hadoop.fs.s3a.vectored.read.max.merged.size", "2097152")
        .config("spark.hadoop.fs.s3a.impl.disable.cache", "true")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.multipart.size", "104857600")
        .config("spark.hadoop.fs.s3a.threads.max", "10")
        .config("spark.hadoop.fs.s3a.connection.maximum", "15")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "3")
        .config("spark.hadoop.fs.s3a.retry.limit", "5")
        .config("spark.hadoop.fs.s3a.retry.interval", "500")
        .config("spark.hadoop.fs.s3a.socket.recv.buffer", "65536")
        .config("spark.hadoop.fs.s3a.socket.send.buffer", "65536")
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .getOrCreate()
    )
    try:
        spark._jvm.java.lang.Class.forName("org.apache.iceberg.spark.SparkCatalog")
        print("Iceberg SparkCatalog OK.")
    except Exception as e:
        print("Không tìm thấy Iceberg SparkCatalog:", e)
        raise

    return spark

def write_spark(df, table_name):
    # write
    return df.write \
        .format("jdbc") \
        .option("url", f"jdbc:oracle:thin:@{SQL_TEST_ORACLE_DOCKER}:{SQL_TEST_PORT}/{SQL_TEST_SERVICE}") \
        .option("dbtable", table_name) \
        .option("user", SQL_TEST_USER) \
        .option("password", SQL_TEST_PASS) \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .option("isolationLevel", "READ_COMMITTED") \
        .mode("append") \
        .save()

def write_spark_iceberg(df, table_name):
    spark = df.sparkSession
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.weather")
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS local.weather.{table_name} (
        dt          STRING,
        temperature FLOAT,
        pressure    FLOAT,
        humidity    FLOAT,
        description STRING
      )
      USING iceberg
      PARTITIONED BY (dt)
      TBLPROPERTIES ('format-version'='2')
    """)
    df.writeTo(f"local.weather.{table_name}").append()

def install_spark_dependencies():
    spark = get_spark()
    print("Spark session started.")
    confs = spark.sparkContext.getConf().getAll()
    for k, v in confs:
        print(f"{k} = {v}")
    print("Spark session finished.")
    spark.stop()


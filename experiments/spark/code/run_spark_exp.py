import os
import time
import socket
from datetime import datetime
from pyspark.sql import functions as F, SparkSession
from pyspark.sql.window import Window as W

PARTITION_BY = ["SOURCE_POINT_NUMBER"]; ORDER_BY = ["SENSOR_NUMBER"]    # SQ1
# PARTITION_BY = ["SENSOR_NUMBER"]; ORDER_BY = ["SOURCE_POINT_NUMBER"]  # SQ2
# PARTITION_BY = ["CROSSLINE"]; ORDER_BY = ["INLINE", "DISTANCE"]       # SQ3
# PARTITION_BY = ["INLINE", "CROSSLINE"]; ORDER_BY = ["DISTANCE"]       # SQ4

CPU_LIMIT = 16
MEMORY_LIMIT = 128
DATA_PATH = "/data"

# SPARK_MASTER = f"local[{CPU_LIMIT}]"    # Local mode
SPARK_MASTER = "spark://node6.bdcl:7077"  # Distributed mode


if __name__ == "__main__":

    print("Starting")

    APP_NAME = "SparkReading"
    LOCAL_IP = socket.gethostbyname(socket.gethostname())
    
    print(f"CPU_LIMIT={CPU_LIMIT}")
    print(f"MEMORY_LIMIT={MEMORY_LIMIT}")
    print(f"PARTITION_BY={PARTITION_BY}, ORDER_BY={ORDER_BY}")

    timeout_value = "900s"
    spark = (
        SparkSession
        .builder
        .appName(APP_NAME)
        .master(f"spark://node6.bdcl:7077")

        .config("spark.driver.host", LOCAL_IP)

        .config("spark.ui.port", "4040")
        .config("spark.driver.bindAddress", "0.0.0.0")

        .config("spark.driver.cores", f"1")
        .config("spark.driver.memory", f"2g")
        .config("spark.executor.cores", f"{CPU_LIMIT}")
        .config("spark.executor.memory", f"{MEMORY_LIMIT}g")
        .config("spark.executor.instances", "8")

        .config("spark.network.timeout", timeout_value)
        .config("spark.storage.blockManagerHeartbeatTimeoutMs", timeout_value)
        .config("spark.shuffle.io.connectionTimeout", timeout_value)
        .config("spark.executor.heartbeatInterval", "20s")
        .config("spark.network.timeoutInterval", "600s")

        .config("spark.memory.fraction", "0.8")
        .config("spark.memory.storageFraction", "0.5")

        .config("spark.network.timeout", "180s")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")

        .getOrCreate()
    )

    df = spark.read.parquet(f"file://{DATA_PATH}")

    start = datetime.utcnow()
    print(f"Started at {start}")

    df.withColumn(
        "row_num",
        F.row_number().over(
            W.partitionBy(
                *[F.col(c) for c in PARTITION_BY]
            ).orderBy(
                *[F.col(c) for c in ORDER_BY]
            )
        )
    ).write.format("noop").mode("overwrite").save()

    finish = datetime.utcnow()

    print(f"Finished at {finish}")
    print(f"Duration: {finish - start}")

    try:
        while True:
            time.sleep(1)
    finally:
        spark.stop()

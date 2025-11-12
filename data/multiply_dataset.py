from datetime import datetime
import socket
from pyspark.sql import functions as F, SparkSession
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql.dataframe import DataFrame as SparkDataFrame


CPU_COUNT = 16
MEMORY_LIMIT = 96
BASE_DATASET_PATH = "/data/base_dataset.parquet"
SAVE_DIR ="/output_dir/multiplied_datasets"
MULTIPLY_COUNT = 10

# Modify only columns related to experiments while dataset multiplying.
COLUMNS_TO_TRANSFORM = [
    'TRACE_SEQUENCE_NUMBER_FILE', 'SOURCE_POINT_NUMBER',
    'SENSOR_NUMBER', 'INLINE', 'CROSSLINE'
]


def init_spark():

    local_ip = socket.gethostbyname(socket.gethostname())

    spark = (
        SparkSession
        .builder
        .appName("Prepare_data")
        .master(f"local[{CPU_COUNT}]")

        .config("spark.driver.host", local_ip)

        .config("spark.ui.port", "4040")
        .config("spark.driver.bindAddress", "0.0.0.0")

        .config("spark.driver.cores", f"{CPU_COUNT}")

        .config("spark.driver.memory", f"{MEMORY_LIMIT}g")

        .config("spark.memory.fraction", "0.8")
        .config("spark.memory.storageFraction", "0.5")

        .config("spark.network.timeout", "180s")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")

        .getOrCreate()
    )

    return spark


def generate_new_part(
        sdf: "SparkDataFrame",
        counter: int,
        columns_without_modify: list[str],
        deltas: dict
):

    return sdf.select(
        *columns_without_modify,
        *[
            # Formula: (delta + 1) * COUNTER + _CURRENT
            ((deltas[key] + 1) * counter + F.col(key)).alias(key)
            for key in COLUMNS_TO_TRANSFORM
        ]
    )


def run():

    spark = init_spark()

    sdf = spark.read.parquet(BASE_DATASET_PATH).cache()

    columns_without_modify = [c for c in sdf.columns if c not in COLUMNS_TO_TRANSFORM]

    # Calc stats
    deltas: dict = sdf.agg(
        *[
            (F.max((col := F.col(key))) - F.min(col)).alias(key)
            for key in COLUMNS_TO_TRANSFORM
        ]
    ).toPandas().loc[0, :].to_dict()

    for i in range(1, MULTIPLY_COUNT):

        # It takes about 7-8 minutes to generate dataset part

        start_dt = datetime.utcnow()

        _ = (
            generate_new_part(sdf=sdf, counter=i, columns_without_modify=columns_without_modify, deltas=deltas)
            .orderBy(F.col("TRACE_SEQUENCE_FILE"))
            .repartition(1)
            .write
            .format("parquet")
            .mode("append")
            .save(f"{SAVE_DIR}/generated_dataset_{i}.parquet")
        )

        print(f"Part {i} processed in {datetime.utcnow() - start_dt}")


if __name__ == "__main__":
    run()

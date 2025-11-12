DATA_PATH=$1
TABLE_NAME="${2:-default.seismic_data}"
CONTAINER_NAME="${3:-clickhouse-server}"

echo "Data path: $DATA_PATH"
echo "Table: $TABLE_NAME"
echo "Container name: $CONTAINER_NAME"


for file in $(ls $DATA_PATH); do

  file_path=$DATA_PATH/$file
  echo "[$(date)] Inserting $file_path"
  cat $file_path | docker container exec -i $CONTAINER_NAME clickhouse-client --query="INSERT INTO $TABLE_NAME FORMAT Parquet" --min_insert_block_size_bytes=8147483648

done

echo "Completed"
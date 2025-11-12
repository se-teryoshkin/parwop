ENV_FILE="${1:-32_2.env}"
declare -a hosts=("node6.bdcl" "node9.bdcl" "node22.bdcl" "node23.bdcl" "node24.bdcl" "node26.bdcl" "node27.bdcl" "node28.bdcl")

echo "Deploy Spark Master"
ssh node6.bdcl "cd /mnt/nfs/parwop/experiments/spark/deployment && docker-compose -f spark.docker-compose.yml --env-file=/mnt/nfs/parwop/experiments/$ENV_FILE up -d spark-master"

echo "Deploy Spark Workers"
for host in "${hosts[@]}"
  do
    echo $host
    ssh $host "cd /mnt/nfs/parwop/experiments/spark/deployment && docker-compose -f spark.docker-compose.yml --env-file=/mnt/nfs/parwop/experiments/$ENV_FILE up -d --build spark-worker"
  done

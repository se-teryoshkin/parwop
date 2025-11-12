ENV_FILE="${1:-32_2.env}"
declare -a hosts=("node6.bdcl" "node9.bdcl" "node22.bdcl" "node23.bdcl" "node24.bdcl" "node26.bdcl" "node27.bdcl" "node28.bdcl")

echo "Deploy coordinator"
ssh node6.bdcl "cd /mnt/nfs/parwop/experiments/parwop/deployment && docker-compose -f parwop.docker-compose.yml --env-file=/mnt/nfs/parwop/experiments/$ENV_FILE --env-file=/data/hdd/experiments-parwop/parwop/worker.env up -d --build parwop-coordinator"

echo "Deploy rabbitmq"
ssh node6.bdcl "cd /mnt/nfs/parwop/experiments/parwop/deployment && docker-compose -f parwop.docker-compose.yml --env-file=/mnt/nfs/parwop/experiments/$ENV_FILE --env-file=/data/hdd/experiments-parwop/parwop/worker.env up -d rabbitmq"

for host in "${hosts[@]}"
  do
    echo $host
    ssh $host "cd /mnt/nfs/parwop/experiments/parwop/deployment && docker-compose -f parwop.docker-compose.yml --env-file=/mnt/nfs/parwop/experiments/$ENV_FILE --env-file=/data/hdd/experiments-parwop/parwop/worker.env up -d --build parwop-worker"
  done

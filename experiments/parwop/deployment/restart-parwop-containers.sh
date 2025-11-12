declare -a hosts=("node6.bdcl" "node9.bdcl" "node22.bdcl" "node23.bdcl" "node24.bdcl" "node26.bdcl" "node27.bdcl" "node28.bdcl")

echo "Restart coordinator"
ssh node6.bdcl "sudo docker container restart parwop-coordinator"

for host in "${hosts[@]}"
  do
    echo $host
    ssh $host "sudo docker container restart parwop-worker"
  done
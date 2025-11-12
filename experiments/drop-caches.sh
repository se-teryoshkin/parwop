declare -a hosts=("node6.bdcl" "node9.bdcl" "node22.bdcl" "node23.bdcl" "node24.bdcl" "node26.bdcl" "node27.bdcl" "node28.bdcl")

for host in "${hosts[@]}"
  do
    echo $host
    ssh $host "sudo sh -c 'echo 3 >  /proc/sys/vm/drop_caches'"
  done
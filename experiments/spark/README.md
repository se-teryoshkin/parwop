# Deployment
Apache Spark is deployed using `docker-compose`.

This repository with the code in our infrastructure is located on an NFS server and mounted to `/mnt/nfs/` on each node, as this allows for convenient configuration and modification of configuration files in one place. However, your setup may differ.

Before running, you need to modify the following files:
- `spark.docker-compose.yml`:
  - Change the `SPARK_MASTER_URL` environment variable for `spark-worker` to the address where the Spark Master will reside. The Spark Master is also listed in this compose file; the default port for `spark://` is `7077`.
  - Change the `volumes` values to your paths. The `/data/hdd` path is located on the node's local HDD.

After completing these modifications, you can deploy Spark either locally (1 node) or in distributed mode.
To run in local mode:
- Run the command `docker compose -f spark.docker-compose.yml --env-file={ENV_FILE} up -d --build spark-local`, where `{ENV_FILE}` is the path to the `.env` file with the specified resource constraints (`parwop/experiments`). This command will also build the image.
- Now, running `docker exec -it spark-local bash` will give you an interactive shell and allow you to run scripts and other commands.

This repository provides four `.env` file options with resource limits:
- 32 GiB RAM + 2 CPU cores (`32_2.env`)
- 64 GiB RAM + 4 CPU cores (`64_4.env`)
- 128 GiB RAM + 8 CPU cores (`128_8.env`)
- 128 GiB RAM + 16 CPU cores (`128_16.env`)

**Note that the container's available RAM has been increased by 10% to ensure that the container's system processes also have enough memory.**

To run in distributed mode:
- Start Spark Master with `docker compose -f spark.docker-compose.yml --env-file={ENV_FILE} up -d spark-master`
- Run Spark Worker on all required nodes using the command `docker compose -f spark.docker-compose.yml --env-file={ENV_FILE} up -d spark-worker`
- Client applications can still be run from the `spark-local` container; you just need to specify the Spark Master address.

For easy deployment of Spark Worker on multiple nodes, the `deploy-spark-all-nodes.sh` script is available. Before using this script, you must change the list of nodes, as well as the path to the `spark.docker-compose.yml` and `.env` files.
**Important: SSH access via a key to all nodes must be configured to use this script.**


# Data Preparation
Since Apache Spark typically uses distributed file systems (e.g., HDFS), all executors must have access to all files. To ensure the integrity of the experiment, we excluded the distributed file system from the experiment and used only the local file system.
Therefore, for the application to function correctly in distributed mode, **EACH WORKER MUST HAVE A FULL COPY OF THE DATA** (however, each executor will only read a portion of the full copy).


# Experiment Details
After each run, the OS page cache must be dropped. To drop the cache on all nodes, you can use the `drop-caches.sh` script. **Note: You must have root privileges to execute commands.**


# Experiment script
Before running the `run_spark_exp.py` script, you must specify the launch mode (local or distributed) and specify the correct address to the Spark Master.

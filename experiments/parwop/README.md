# Deployment
PARWOP is deployed using `docker-compose`.

This code repository within our infrastructure is located on an NFS server and mounted to `/mnt/nfs/` on each node, as this allows for convenient configuration and modification of configuration files in one place. However, your setup may differ.

Before starting, you must complete the following steps:
- Modify `parwop.docker-compose.yml`:
  - Change the `COORDINATOR_ADDRESS` environment variable to the address where the PARWOP Coordinator will be located. The PARWOP Coordinator is also present in this compose file; the default port is `8000`.
  - Change the `RABBITMQ_HOST` value to the host where RabbitMQ is currently or will be located. RabbitMQ is also present in the compose file; the default port is `5672`.
  - Change the `volumes` values to your paths. The `/data/hdd` path is located on the node's local HDD.
- Create `worker.env` files: On each node that will act as a PARWOP worker, you must create a `worker.env` file with the following environment variables: `PARWOP_NODE_ID` - a unique worker identifier (e.g., `worker-1`) and `WORKER_ADDRESS` - the HTTP address where the worker API will be accessible (e.g., `http://node6.bdcl:8001`). The default port is `8001`.

After completing these steps, you can deploy PARWOP.

To do this, run the following commands:
- Start RabbitMQ: `docker-compose -f parwop.docker-compose.yml --env-file={GENERAL_ENV_FILE} --env-file={WORKER_ENV_FILE} up -d rabbitmq`, where `{GENERAL_ENV_FILE}` is the path to the `.env` file with the described resource limits (`parwop/experiments`), and `{WORKER_ENV_FILE}` is the path to the worker's `.env` file (if RabbitMQ is deployed on one of the worker nodes). **Note: Although these `.env` files are not required for starting RabbitMQ, `docker compose` will not work correctly without them. You can copy the RabbitMQ service definition to a separate file and run it without depending on `.env` files.**
- Run PARWOP Coordinator: `docker-compose -f parwop.docker-compose.yml --env-file={GENERAL_ENV_FILE} --env-file={WORKER_ENV_FILE} up -d --build parwop-coordinator`. `{GENERAL_ENV_FILE}` is required, while `{WORKER_ENV_FILE}` is optional (however, without it, the PARWOP Coordinator service will not start). **The solution in this case is the same as for RabbitMQ - copy the definition to a separate file and run it from there. Then there will be no dependency on `{WORKER_ENV_FILE}`.** This command will also build the image.
- Run PARWOP Worker on each worker node: `docker-compose -f parwop.docker-compose.yml --env-file={GENERAL_ENV_FILE} --env-file={WORKER_ENV_FILE} up -d --build parwop-worker`. All `.env` files are required. This command will also build the image.

This repository provides four `.env` file variants with resource limits:
- 32 GiB RAM + 2 CPU cores (`32_2.env`)
- 64 GiB RAM + 4 CPU cores (`64_4.env`)
- 128 GiB RAM + 8 CPU cores (`128_8.env`)
- 128 GiB RAM + 16 CPU cores (`128_16.env`)

**It's worth noting that the container's available RAM has been increased by 10% to ensure sufficient memory for the container's system processes.**

For convenient management of PARWOP on multiple nodes, the scripts `deploy-parwop-all-nodes.sh`, `stop-parwop-containers.sh`, and `restart-parwop-containers.sh` are available. Before using the script data, you must modify the node list and paths to `parwop.docker-compose.yml` and all `.env` files.
**Important: To use this script, you must have configured SSH key access to all nodes.**

To execute local requests, you can use any worker by getting its interactive shell with the command: `docker container exec -it parwop-worker bash`.

To execute distributed requests, you can use either worker containers or the coordinator.


# Experiment Details
After each run, the OS page cache must be dropped. To drop the cache on all nodes, you can use the `drop-caches.sh` script. **Note: You must have root privileges to execute commands.**


# Experiment Script
Before running the `run_parwop_exp.py` script, you must specify the launch mode (local or distributed), a list of node IDs, and specify the correct address to the PARWOP Coordinator. Additionally, you must calculate and set the correct `MEMORY_LIMIT` for queries.

# Deployment
Clickhouse is deployed using `docker-compose`.

This repository within our infrastructure is located on an NFS server and mounted to `/mnt/nfs/` on each node, as this allows for convenient configuration and modification of configuration files in one place. However, your setup may differ.

Before starting the server, you need to modify the following files:
- `deployment/ch.docker-compose.yml`: You will need to change the paths in `volumes` to match your own. The `/data/hdd` path is located on the node's local HDD.
- `deployment/config.xml`:
  - You need to edit the `clickhouse.zookeeper` section to specify either your zookeeper instance (and remove the `clickhouse.keeper_server` section) or the address of one of the Clickhouse servers (in this case, this server must also be configured).
  - You need to modify the `clickhouse.remote_servers.experimental_cluster` section to list all the Clickhouse nodes you plan to use.

After making these modifications, you can deploy the server using the command `docker compose -f ch.docker-compose.yml --env-file={ENV_FILE} up -d clickhouse-server`, where `{ENV_FILE}` is the `.env` file with container restrictions.

This repository offers four configurations:

- 32 GiB RAM + 2 CPU cores (`32_2.env`)
- 64 GiB RAM + 4 CPU cores (`64_4.env`)
- 128 GiB RAM + 8 CPU cores (`128_8.env`)
- 128 GiB RAM + 16 CPU cores (`128_16.env`)

**It's worth noting that the container's available RAM has been increased by 10% to ensure sufficient memory for the container's system processes.**

For working with multiple nodes, the `deploy-clickhouse-all-nodes.sh` script has been prepared, allowing you to deploy Clickhouse server on each node. Before using it, you need to modify the script: list the nodes and adjust the path to the `ch.docker-compose.yml` directory. **IMPORTANT: `ch.docker-compose.yml` and the corresponding `.env` file must be accessible on each node.** You will also need configured SSH with key-based authentication to all specified nodes.
This also applies to the `restart-clickhouse-containers.sh` script.


# Preparing the data
The data to be loaded into Clickhouse should already be generated (see `parwop/data/README.md`). Before loading, you need to create a table with the structure specified in `experiments/clickhouse/data/table-schema.sql`. When working with multiple nodes, it's also useful to create `Distributed` tables (replace `N_NODES` with the number of your nodes before running the command to create the `Distributed` table).

After creating the tables, you can use the `experiments/clickhouse/data/insert_data.sh` script, passing it the directory with the Parquet files. If you need to insert data into a `Distributed` table (to distribute records across the cluster), you must also pass the name of the `Distributed` table; otherwise, the data will be inserted into the node's local table. **The `insert_data.sh` script must be run from the node hosting one of the Clickhouse cluster containers!**

Examples of execution:
- `insert_data.sh /path/to/my_dir_with_parquet` (inserts all Parquet files into the `default.seismic_data` table)
- `insert_data.sh /path/to/my_dir_with_parquet default.distr_seismic_data` (inserts all Parquet files into the `default.distr_seismic_data` table)

After all files have been inserted, it is recommended to wait for all merge processes to complete and then call the `OPTIMIZE TABLE default.seismic_data FINAL` procedure (if working with multiple nodes, this can be used with the `ON CLUSTER experimental_cluster` clause), which will merge all the Parquet tables into one.


# Experiment Details
Since the data volume significantly exceeded the available memory (in almost all experiments performed in the paper), Clickhouse terminated with an OutOfMemory error when executing window functions. To ensure this query could be executed, a manual external sorting approach was essentially used: all table data was written to a new table whose key was a composite of the query's `PARTITION BY` and `ORDER BY` clauses. To prevent the table's Merge processes from degrading I/O performance, Merge processes were "disabled" in the new table using the `max_bytes_to_merge_at_max_space_in_pool` setting (this setting limits the size of the partition after the Merge process, and when set to a low value, it effectively disables the table's Merge processes).

Then, reading is performed from the new table or, if multiple nodes are used, from a `Distributed` table, whose local table data is combined using a sort-merge manner.


# Experiment Script
`ch.docker-compose.yml` also contains the specification of the container with the Python client used to perform experimental runs. You should change the path in `volumes` to match your own.
This container is launched using `docker compose -f ch.docker-compose.yml --env-file=/mnt/nfs/parwop/experiments/32_2.env up -d --build clickhouse-client-container`. You can then get an interactive container shell using `docker container exec -it clickhouse-client-container bash` and then run the testing script and other commands.

An example of running a script that will execute a request is `python -u run_clickhouse_exp.py`. Before running the script, you must specify the request keys, resource limits, and a list of nodes.

The OS page cache must be dropped after each run. To drop the cache on all nodes, you can use the `drop-caches.sh` script. **Note: You must have superuser permissions to execute commands.**

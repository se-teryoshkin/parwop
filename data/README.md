The experiment used synthetic datasets generated from an anonymized dataset. The anonymized dataset is represented by the file `base_dataset.parquet` and is not available in the repository because it is approximately 1 GB in size. It can be downloaded from the following link: https://drive.google.com/file/d/1dsY0_aqMwudianQT4fk1YDK75dyOi_ql/view?usp=sharing

The dataset does not contain seismic trace amplitude values; these must be generated manually (how to generate amplitudes is described below).

For the multi-node experiment, `base_dataset.parquet` was multiplied x10, preserving relationships in columns important for the experiment.

Dataset multiplication was performed using the `multiply_dataset.py` script.

To split the dataset into a set of Parquet files and generate seismic trace amplitudes, the `split_and_generate_traces.py` script was used.

This script takes a directory containing Parquet files as input and then saves the dataset in parts to disk along with the generated amplitudes.
**WARNING! Running this script will write over 350 GB of data to disk!**

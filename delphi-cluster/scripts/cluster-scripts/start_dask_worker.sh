#!/usr/bin/bash

# This is a template, adapt as needed
# 1. Get the hostname of the scheduler and the number of cores from argument
scheduler_hostname=$1
nprocs=$2

echo "Connecting Dask worker $HOSTNAME to tcp://${scheduler_hostname}:8786"

# 2. Source the correct Python environment, i.e. with which ROOT was installed
source $HOME/mambaforge/etc/profile.d/conda.sh
conda activate distrdf-env

# 3. This is optional. In case using a custom DistRDF module, put it first in
# PYTHONPATH so that it gets precedence over the one from the ROOT installation
export PYTHONPATH=$HOME/distrdf-project:$PYTHONPATH

# 4. Start the worker, connecting it to the scheduler. `nthreads` should always
# be equal to 1. `nprocs` can change, depending on the benchmark.
nohup dask-worker "tcp://${scheduler_hostname}:8786" --nprocs $nprocs --nthreads 1 1>$HOME/dask_worker_nohup.out 2>$HOME/dask_worker_nohup.err &

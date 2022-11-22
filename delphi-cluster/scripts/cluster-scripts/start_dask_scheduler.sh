#!/usr/bin/bash
echo "Starting Dask scheduler $HOSTNAME"

# This is a template, adapt as needed
# 1. Source the correct Python environment, i.e. with which ROOT was installed
source $HOME/mambaforge/etc/profile.d/conda.sh
conda activate distrdf-env

# 2. This is optional. In case using a custom DistRDF module, put it first in
# PYTHONPATH so that it gets precedence over the one from the ROOT installation
export PYTHONPATH=$HOME/distrdf-project:$PYTHONPATH

# 3. Start the Dask scheduler
nohup dask-scheduler 1>$HOME/dask_scheduler_nohup.out 2>$HOME/dask_scheduler_nohup.err &

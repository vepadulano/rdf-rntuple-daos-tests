#!/usr/bin/bash
# This is a template, adapt as needed
# 1. Get the hostname of the scheduler from argument
scheduler_hostname=$1

# 2. Set a directory of the scripts that will actually launch the dask scheduler
# or worker on the node
clusterscriptsdir="~/cluster-scripts"

# 3. Launch the script via ssh
if [ -n "${scheduler_hostname}" ]
then
  ssh $scheduler_hostname "${clusterscriptsdir}/start_dask_scheduler.sh"
else
  echo "Please supply a hostname for the scheduler"
fi

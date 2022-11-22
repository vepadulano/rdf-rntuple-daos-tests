#!/usr/bin/bash
# This is a template, adapt as needed
# 1. Get the hostname of the scheduler from argument. The number of nodes here
# is just to have a number to iterate on. This could become a list of hostnames,
# must be adapted to the cluster. Also get the number of cores to be used on
# each node.
scheduler_hostname=$1
nodes=$2
corespernode=$3

# 2. Set a directory of the scripts that will actually launch the dask scheduler
# or worker on the node
clusterscriptsdir="~/cluster-scripts"

# 3. Launch a Dask worker on each node
if [ -n "${scheduler_hostname}" ]
then
# This loop assumes that workers have the same name with an increasing identifier.
# Not true in general, adapt here to worker hostnames
  for i in `seq 1 $((nodes))`
  do
  ssh WORKER$i "${clusterscriptsdir}/start_dask_worker.sh ${scheduler_hostname} ${corespernode}"
  done
else
  echo "Please supply a hostname for the scheduler"
fi

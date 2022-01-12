# Source right environment
source /mnt/cern/vpadulan/activate_conda_rootdev.sh
source /mnt/cern/vpadulan/rootinstall/rootdev_release/bin/thisroot.sh

DAOSPOOL=eb518f08-ab9d-4599-9388-acb128febc12

for corespernode in 16
do

for nodes in 6
do
    # Stop Dask cluster and restart again with correct amount of nodes
    bash /mnt/cern/vpadulan/dask/launch-scripts/clean_processes.sh
    bash /mnt/cern/vpadulan/dask/launch-scripts/start_dask_scheduler.sh hl-d104
    bash /mnt/cern/vpadulan/dask/launch-scripts/start_dask_workers.sh hl-d104 $nodes $corespernode

    python distrdf_lhcb_multinode.py --storagetype daos --daospool $DAOSPOOL --nodes $nodes --corespernode $corespernode

    rm -rf dask-worker-space
    mkdir -p timeresults_800x_${nodes}nodes_${corespernode}corespernode
    mv /mnt/cern/vpadulan/dask-task-metrics/* timeresults_800x_${nodes}nodes_${corespernode}corespernode/
done

done

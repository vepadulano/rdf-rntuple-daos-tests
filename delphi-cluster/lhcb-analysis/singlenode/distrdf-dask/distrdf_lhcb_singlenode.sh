# Source right environment
source /mnt/cern/vpadulan/activate_conda_rootdev.sh
source /mnt/cern/vpadulan/rootinstall/rootdev_release/bin/thisroot.sh

DAOSPOOL=8691993b-60c4-4ced-ba25-ae062b2ce8f0

for cores in 16 14 12 10 8 6 4 2 1
do

    # Stop Spark cluster and restart again with correct amount of nodes
    bash /mnt/cern/vpadulan/dask/launch-scripts/clean_processes.sh
    bash /mnt/cern/vpadulan/dask/launch-scripts/start_dask_scheduler.sh hl-d104
    bash /mnt/cern/vpadulan/dask/launch-scripts/start_dask_workers.sh hl-d104 1 $cores

    python distrdf_lhcb_singlenode.py --storagetype daos --daospool $DAOSPOOL --nchunks $cores --cores $cores

    rm -rf dask-worker-space
    mkdir -p timeresults_200x_${cores}cores
    mv /mnt/cern/vpadulan/dask-task-metrics/* timeresults_200x_${cores}cores/


done

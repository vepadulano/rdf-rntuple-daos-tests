# Source right environment
source /mnt/cern/vpadulan/activate_conda_rootdev.sh
source /mnt/cern/vpadulan/rootinstall/rootdev_release/bin/thisroot.sh

DAOSPOOL=eb632dd4-732a-4df3-9f5a-f4d14014dd56

export DISTRDF_NTUPLEMETRICS_DIR=$PWD

for cores in 1
do

    # Stop Spark cluster and restart again with correct amount of nodes
    bash /mnt/cern/vpadulan/spark/launch-scripts/stop_spark_master.sh
    bash /mnt/cern/vpadulan/spark/launch-scripts/stop_spark_workers.sh
    bash /mnt/cern/vpadulan/spark/launch-scripts/start_spark_master.sh
    bash /mnt/cern/vpadulan/spark/launch-scripts/start_spark_workers_8cores.sh hl-d104 1

    python distrdf_lhcb_singlenode_createhisto.py --storagetype daos --daospool $DAOSPOOL --nchunks $cores --cores $cores

    mkdir -p timeresults_200x_${cores}cores
    mv distrdf_task* timeresults_200x_${cores}cores/
done


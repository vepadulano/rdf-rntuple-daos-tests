# Source right environment
source /mnt/cern/vpadulan/activate_conda_rootdev.sh
source /mnt/cern/vpadulan/rootinstall/rootdev_release/bin/thisroot.sh

DAOSPOOL=b703792b-b004-4ab8-94fe-515db1e5396f

export DISTRDF_NTUPLEMETRICS_DIR=$PWD

for corespernode in 8
do

for nodes in 7
do
    # Stop Spark cluster and restart again with correct amount of nodes
    bash /mnt/cern/vpadulan/spark/launch-scripts/stop_spark_master.sh
    bash /mnt/cern/vpadulan/spark/launch-scripts/stop_spark_workers.sh
    bash /mnt/cern/vpadulan/spark/launch-scripts/start_spark_master.sh
    bash /mnt/cern/vpadulan/spark/launch-scripts/start_spark_workers_8cores_7nodes.sh hl-d104 $nodes

    python distrdf_lhcb_multinode.py --storagetype daos --daospool $DAOSPOOL --nodes $nodes --corespernode $corespernode

    mkdir -p timeresults_200x_${nodes}nodes_${corespernode}corespernode
    mv distrdf_task* timeresults_200x_${nodes}nodes_${corespernode}corespernode/
done

done

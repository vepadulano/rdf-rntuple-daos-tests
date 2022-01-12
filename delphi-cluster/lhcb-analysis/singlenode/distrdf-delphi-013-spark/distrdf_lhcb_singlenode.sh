# Source right environment
source /mnt/cern/vpadulan/activate_conda_rootdev.sh
source /mnt/cern/vpadulan/rootinstall/rootdev_release/bin/thisroot.sh

DAOSPOOL=6c29fe7f-9fd7-4dec-b9bb-d568957b7c8f

export DISTRDF_NTUPLEMETRICS_DIR=$PWD

# Start Spark master and 1 worker on 1 NUMA domain
ssh delphi-013 "bash /mnt/cern/vpadulan/spark/launch-scripts/stop_spark_worker.sh"
bash /mnt/cern/vpadulan/spark/launch-scripts/stop_spark_master.sh

bash /mnt/cern/vpadulan/spark/launch-scripts/start_spark_master.sh
ssh delphi-013 "bash /mnt/cern/vpadulan/spark/launch-scripts/start_delphi013_1numa.sh hl-d101"

for cores in 1 2 4 8 16 24 32
do
    python distrdf_lhcb_singlenode.py --storagetype daos --daospool $DAOSPOOL --nchunks $cores --coresperworker $cores --nworkers 1
    mkdir -p timeresults_100x_${cores}cores
    mv distrdf_task* timeresults_100x_${cores}cores/
done

ssh delphi-013 "bash /mnt/cern/vpadulan/spark/launch-scripts/stop_spark_worker.sh"
bash /mnt/cern/vpadulan/spark/launch-scripts/stop_spark_master.sh


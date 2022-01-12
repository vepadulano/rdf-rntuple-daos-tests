# Source right environment
source /mnt/cern/vpadulan/activate_conda_rootdev.sh
source /mnt/cern/vpadulan/rootinstall/rootdev_release/bin/thisroot.sh

DAOSPOOL=6c29fe7f-9fd7-4dec-b9bb-d568957b7c8f

export DISTRDF_NTUPLEMETRICS_DIR=$PWD

for cores in 8 6 4 2 1
do
    python distrdf_lhcb_singlenode.py --storagetype daos --daospool $DAOSPOOL --nchunks $cores --cores $cores
    mkdir -p timeresults_100x_${cores}cores
    mv distrdf_task* timeresults_100x_${cores}cores/
done

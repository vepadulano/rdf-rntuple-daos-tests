# Source right environment
source /mnt/cern/vpadulan/activate_conda_rootdev.sh
source /mnt/cern/vpadulan/rootinstall/rootdev_release/bin/thisroot.sh

DAOSPOOL=6c29fe7f-9fd7-4dec-b9bb-d568957b7c8f

for nchunks in 16 14 12 10 8 6 4 2 1
do
    python multiproc_lhcb.py --storagetype daos --daospool $DAOSPOOL --nchunks $nchunks
    mkdir -p timeresults_100x_${nchunks}processes
    mv multiproc_lhcb_process*  timeresults_100x_${nchunks}processes/
done


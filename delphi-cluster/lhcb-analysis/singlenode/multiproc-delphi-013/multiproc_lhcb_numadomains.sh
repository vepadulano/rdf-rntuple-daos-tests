# Source right environment
source /mnt/cern/vpadulan/activate_conda_rootdev.sh
source /mnt/cern/vpadulan/rootinstall/rootdev_release/bin/thisroot.sh

DAOSPOOL=6c29fe7f-9fd7-4dec-b9bb-d568957b7c8f

for nchunks in 64 56 48 32 24 16 8 4 2 1
do
    python multiproc_lhcb_numadomains.py --storagetype daos --daospool $DAOSPOOL --nchunks $nchunks
    wait
    mkdir -p timeresults_100x_${nchunks}processes
    mv multiproc_lhcb_process*  timeresults_100x_${nchunks}processes/
done


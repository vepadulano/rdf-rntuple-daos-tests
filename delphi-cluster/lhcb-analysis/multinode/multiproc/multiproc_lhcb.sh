# Source right environment
source /mnt/cern/vpadulan/activate_conda_rootdev.sh
source /mnt/cern/vpadulan/rootinstall/rootdev_release/bin/thisroot.sh

DAOSPOOL=6c29fe7f-9fd7-4dec-b9bb-d568957b7c8f

for nchunks in 8
do
    python multiproc_lhcb.py --storagetype daos --daospool $DAOSPOOL --nchunks $nchunks
    mkdir -p timeresults_50x_${nchunks}processes_`hostname`
    mv multiproc_lhcb_`hostname`_*  timeresults_50x_${nchunks}processes_`hostname`/
done


# Source right environment
source /mnt/cern/vpadulan/activate_conda_rootdev.sh
source /mnt/cern/vpadulan/rootinstall/rootdev_release/bin/thisroot.sh

DAOSPOOL=b703792b-b004-4ab8-94fe-515db1e5396f

for nchunks in 16 8
do
    python multiproc_lhcb_numadomains.py --storagetype daos --daospool $DAOSPOOL --nchunks $nchunks
    mkdir -p timeresults_numadomains_200x_${nchunks}processes
    mv multiproc_lhcb_process*  timeresults_numadomains_200x_${nchunks}processes/
done


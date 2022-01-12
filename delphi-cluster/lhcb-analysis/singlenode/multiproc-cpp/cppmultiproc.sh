# Source right environment
source /mnt/cern/vpadulan/activate_conda_rootdev.sh
source /mnt/cern/vpadulan/rootinstall/rootdev_release/bin/thisroot.sh

DAOSPOOL=a63bbe83-e2d9-4740-ba4b-54e9f860140d

python cppmultiproc.py --storagetype daos --daospool $DAOSPOOL --nchunks 16



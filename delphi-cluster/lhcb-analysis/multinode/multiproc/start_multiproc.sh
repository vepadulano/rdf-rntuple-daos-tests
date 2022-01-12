for name in hl-d101 hl-d102
do
  ssh $name "/mnt/cern/vpadulan/multiproc-multinode-lhcb/multiproc_lhcb.sh" &
done

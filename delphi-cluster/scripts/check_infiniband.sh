nodes=(
hl-d101
hl-d102
hl-d103
hl-d104
hl-d105
hl-d106
hl-p201
hl-p204
)

for node in "${nodes[@]}"
do
  ssh $node "echo ; echo \`hostname\`; ibstatus mlx5_0; ibdev2netdev | grep mlx5_0; cat /sys/class/net/ib1/device/numa_node; lscpu | grep \"Model name:\"; lscpu | grep NUMA; echo "
done

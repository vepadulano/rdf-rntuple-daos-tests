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
  ssh $node "echo \`hostname\`; echo; tmux kill-server; echo; tmux ls"
done

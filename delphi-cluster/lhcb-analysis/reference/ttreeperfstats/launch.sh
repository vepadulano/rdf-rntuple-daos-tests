#!/bin/bash                                                                                                                                       
# Activate conda
eval "$($HOME/mambaforge/bin/conda shell.bash hook)"
conda init
conda activate root626
 
echo "ROOT Installation: " `which root.exe`                                                                                                       
echo "Python executable: " `which python` 

# -u to avoid Python buffering, helps with 'tee'
python -u rdf_lhcb_ttree.py

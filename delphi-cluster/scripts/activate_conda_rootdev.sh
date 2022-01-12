eval "$(/mnt/cern/vpadulan/mambaforge/bin/conda shell.bash hook)"

conda activate rootdev

# Spark things
export SPARK_HOME=/usr/local/spark-3.2.0-bin-hadoop3.2
export SPARK_CONF_DIR=/mnt/cern/vpadulan/spark/conf
export PYSPARK_PYTHON=`which python`

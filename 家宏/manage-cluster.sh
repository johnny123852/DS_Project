#!/bin/bash
set -x
set -e

if [ "$#" -lt 1 ]; then
echo "usage: $0 [start|stop]"
exit -1
fi

BASE_DIR=`dirname $0`
cd $BASE_DIR

COMMAND=$1

case $COMMAND in
 start)
    WORKERS="--num-workers=3"
    gcloud dataproc clusters create cluster-1 \
    --zone asia-northeast1-b \
    --initialization-actions=\
gs://dataproc-initialization-actions/jupyter/jupyter.sh \
    --initialization-action-timeout=30m \
    $WORKERS \
    --worker-machine-type=n1-standard-1 \
    --master-machine-type=n1-standard-1
    ;;
  stop)
    yes | gcloud dataproc clusters delete cluster-1
    ;;
  connect)
    gcloud compute ssh cluster-1-m --project=mozzan-world --zone=asia-northeast1-b -- -D 1080 -N
    ;;
  open)
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"   --proxy-server="socks5://localhost:1080"   --user-data-dir="/tmp/cluster-1-m" http://cluster-1-m:8123
    ;;
  submit)
    gcloud dataproc jobs submit pyspark --cluster=cluster-1 --project=mozzan-world ./bitcoin-gcp-final-graph-2.py

    ;;
  check_and_terminate)
    gcloud compute scp ~/git/btc-analysis/softwares/should_terminate_cluster.python cluster-1-m:
    SHOULD_TERMINATE=`gcloud compute ssh cluster-1-m --command='python should_terminate_cluster.python'`
    echo "Should Terminate: $SHOULD_TERMINATE"
    if [[ "$SHOULD_TERMINATE" == 1 ]]; then
        yes | gcloud dataproc clusters delete cluster-1
    fi
esac

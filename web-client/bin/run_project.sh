#!/bin/bash

sh ./run_cluster.sh
nohup sh ./run_web.sh >../logs/log 2>&1 &

#!/bin/bash
pkill -f bili.py
nohup python3 -u /root/blive/bili.py > /root/blive/info.log 2>&1 &

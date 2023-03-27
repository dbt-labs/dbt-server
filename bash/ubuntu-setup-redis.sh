#!/bin/bash
# Bash scripts to install redis server in ubuntu image.
sudo apt update
sudo apt install lsb-release redis -y
/etc/init.d/redis-server start

#!/bin/bash

echo "Seting up the container environment..."
echo "Installing dependencies..."
apt-get update > /dev/nell
apt-get install python3 -y > /dev/null
apt-get install python3-pip -y > /dev/null

pip install -r /olist_dw/requirements.txt > /dev/null

echo "Dependencies installed. Starting script..."
echo
python3 /olist_dw/run.py

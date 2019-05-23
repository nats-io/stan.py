#!/bin/bash

export PYTHONPATH=$(pwd)
pip install --upgrade pip
pip install protobuf==3.7
pip install asyncio-nats-client
python tests/test.py

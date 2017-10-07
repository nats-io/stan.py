#!/bin/bash

export PYTHONPATH=$(pwd)
pip install --upgrade pip
pip install protobuf
pip install asyncio-nats-client
python tests/test.py

#!/bin/sh

set -e

VERSION="v0.5.0"

if [ ! "$(ls -A $HOME/nats-streaming-server)" ]; then
  mkdir -p $HOME/nats-streaming-server
  cd $HOME/nats-streaming-server
  wget https://github.com/nats-io/nats-streaming-server/releases/download/$VERSION/nats-streaming-server-$VERSION-linux-amd64.zip -O nats-streaming-server.zip
  unzip nats-streaming-server.zip
  mv nats-streaming-server-$VERSION-linux-amd64/nats-streaming-server nats-streaming-server
else
  echo 'Using cached directory.';
fi

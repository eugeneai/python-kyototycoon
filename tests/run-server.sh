#!/bin/bash

. ./kyoto

/usr/bin/ktserver -dmn -pid /run/kyoto/kyoto.pid -log /var/log/kyoto.log $KTSERVER_OPTS
#!/bin/bash

go build -o mrsequential mrsequential.go

dlv --listen=:40000 --headless=true --api-version=2 --accept-multiclient exec ./mrsequential wc.so pg*.txt
#!/bin/bash -x
find ./espidf-tutorial -type d -name "build" -exec du -hs {} \;
find ./espidf-tutorial -type d -name "build" -exec rm -rf {} \;
find ./rust-tutorials -type d -name "target" -exec du -hs {} \;
find ./rust-tutorials -type d -name "target" -exec rm -rf {} \;

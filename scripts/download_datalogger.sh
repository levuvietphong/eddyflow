#!/bin/bash

dest="Download"
mkdir -p $dest
lftp 63.46.27.48 -e "mirror --verbose --only-newer --continue / $dest ; bye"

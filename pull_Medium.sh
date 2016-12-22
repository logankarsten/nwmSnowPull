#!/bin/sh

# Source bash environment
. $HOME/.profile

# Pull NWM data using Python programs 

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory

cd /d4/karsten/NWM/programs
python pull_Medium.py

exit 0

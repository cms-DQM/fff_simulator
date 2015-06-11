#! /bin/bash

# Given the limited scope of the simulator, this install script is intended
# to be extremely simple.
# You should run it, with root permissions, on the server where you want to
# install.

# cd to the install script's directory
cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Service script
cp ../python/fff_simulator /etc/init.d/
# The actual code
mkdir -p /opt/fff_simulator/
cp ../python/*.py /opt/fff_simulator/
# Configuration
cp ../config/fff_simulator.conf /etc/

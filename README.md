# fff_simulator
The FFF Simulator (previously called roll script) simulates the behaviour of the DAQ system for DQM.

It copies a predefined set of streamer files and pb files to the ramdisk at
a constant rate, simulating consecutive runs, each with multiple lumisections.

The FFF Simulator is started as a daemon by the root user on the BU.
This way anyone can start and stop the simulation at any time.
Do: "service fff_simulator start|stop|restart"
It also allows users to easily check the status of the daemon.
Do: "service fff_simulator status"

We assume that it is executed on the BU.

The settings of the DAQ Simulator are defined in /etc/fff_simulator.conf
The most important setting is obviously the location of the run to simulate.
More information on the settings can be found in the configuration file.

Output and status information is written to a log file (see "Logging
configuration"). Check the config files for the location of the log.

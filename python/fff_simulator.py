# The FFF Simulator (previously called roll script) simulates the behaviour of
# the DAQ system for DQM.
#
# It copies a predefined set of streamer files and pb files to the ramdisk at
# a constant rate, simulating consecutive runs, each with multiple lumisections.
#
# The FFF Simulator is started as a daemon by the root user on the BU.
# This way anyone can start and stop the simulation at any time.
# Do: "service fff_simulator start|stop|restart"
# It also allows users to easily check the status of the daemon.
# Do: "service fff_simulator status"
#
# We assume that it is executed on the BU.
#
# The settings of the DAQ Simulator are defined in /etc/fff_simulator.conf
# The most important setting is obviously the location of the run to simulate.
# More information on the settings can be found in the configuration file.
#
# Output and status information is written to a log file (see "Logging
# configuration" below). Check the config files for the location of the log.

CONFIGURATION_FILE = '/etc/fff_simulator.conf'

import os
import re
import sys
import time
import glob
import json
import shutil
import logging
import ConfigParser
import fff_os_operations
from daemon import Daemon
from subprocess import Popen, PIPE

# Logging configuration:
# We try to determine the location of the log file from the config file
config = ConfigParser.RawConfigParser()
config.read(CONFIGURATION_FILE)
log_file_name = ''
try:
    log_file_name = config.get('Logging', 'LogFile')
except:
    print "Could not get logging config from %s" % CONFIGURATION_FILE
# Otherwise we take just the program name + .log as a fallback
if not log_file_name:
    log_file_name = sys.argv[0] + ".log"
# Configure the actual logging:
logging.basicConfig(filename=log_file_name,
                    format='%(asctime)s %(levelname)s: %(message)s',
                    level=logging.DEBUG)

class FFFSimulator(Daemon):

    def run(self):
        # Load config variables to local variables for easy usage
        cfg = FFFSimulator.load_configuration()

        # Here we start the actual program flow:
        FFFSimulator.assert_run_is_available(cfg.source_run,
                                             cfg.source_dir,
                                             cfg.alternative_source_dirs)
        if FFFSimulator.assert_data_can_be_used(cfg.source_run,
                                                cfg.source_dir):
          fff_os_operations.hltd_stop()
          fff_os_operations.hltd_stop(cfg.fu_host_name)
          fff_os_operations.clean_ramdisk(cfg.ramdisk_dir)
          fff_os_operations.clean_fu_data_dir(cfg.fu_host_name,
                                              cfg.fu_data_dir)
          fff_os_operations.hltd_start()
          fff_os_operations.hltd_start(cfg.fu_host_name)
          # After the start of hltd, we wait 5 seconds before creating our
          # first run, otherwise hltd is no way fast enough to pick it up.
          time.sleep(5)
          FFFSimulator.start_simulating(cfg.source_run, cfg.source_dir,
                                        cfg.ramdisk_dir, cfg.run_key,
                                        cfg.seconds_per_lumi)

    @staticmethod
    def assert_run_is_available(source_run, source_dir,
                                alternative_source_dirs):
        if not FFFSimulator.is_source_run_in_source_dir(source_run,
                                                        source_dir):
            # Try each of the alternative source dirs to find the run:
            found_the_run = False
            for alternative_source_dir in alternative_source_dirs.split(';'):
                if FFFSimulator.is_source_run_in_source_dir(source_run,
                              alternative_source_dir):
                    # If we cannot find the source run in the source dir, but
                    # we did find it in an alternative source dir, then we can
                    # copy it from the alterative source dir to the source dir
                    FFFSimulator.copy_run(source_run,
                                          alternative_source_dir,
                                          source_dir)
                    found_the_run = True
                    break
            # If we cannot find the source run in either the source dir or any
            # of the aternative source dirs, there is nothing we can do and we
            # exit.
            if not found_the_run:
                logging.error("Could not get source run. Exiting.")
                sys.exit(0)

    @staticmethod
    def is_source_run_in_source_dir(source_run, source_dir):
        # First we make sure the source_dir exists and create it if necessary
        if not os.path.exists(source_dir):
            os.makedirs(source_dir, 0755)
        # We assume that if the directory for the run exists, it's actually
        # there. We don't care whether it's empty or not. This will be handled
        # later.
        if os.path.exists(os.path.join(source_dir, source_run)):
            logging.info("Found source %s in %s" % (source_run,
                                                    source_dir))
            return True
        else:
            logging.info("Could not find source %s in %s" % (source_run,
                                                             source_dir))
            return False

    @staticmethod
    def copy_run(run_dir, from_dir, to_dir):
        # This method is intended to copy the playback run data from the
        # fff/output disk to the fff/ramdisk if it's not there, e.g. after
        # a restart of the machine.
        logging.info("Copying %s from %s to %s" % (run_dir, from_dir, to_dir))
        logging.info("For big runs this can take a while.")
        shutil.copytree(os.path.join(from_dir, run_dir),
                        os.path.join(to_dir, run_dir))
        logging.info("Finished copying.")

    @staticmethod
    def assert_data_can_be_used(source_run, source_dir):
        streams = FFFSimulator.get_streams(source_run, source_dir)
        # If there is a stream with no data, we shouldn't continue.
        if [stream for stream in streams if len(stream) == 0]:
            logging.error('Not all the streams have data. Need data in all 3 '
                          'of the streams.')
            return False
        # We need to be able to decide on the run number, otherwise we shouldn't
        # continue either.
        if not FFFSimulator.get_run_number_from_streams(streams):
            return False
        # This concludes the checks.
        logging.info("Scanned the data and it looks okay.")
        return True

    @staticmethod
    def get_streams(source_run, source_dir):
        # We basically need to simulate 3 streams.
        # So we look at all the files and see for each stream what we can find.
        streamDQM = []
        streamDQMHistograms = []
        streamDQMCalibration = []
        # We, in fact, look primarily at the .jsn files first.
        full_search_path = os.path.join(source_dir, source_run, '*.jsn')
        for json_file in glob.glob(full_search_path):
            # We explicitely don't use any fancy regular expressions here:
            # (In fact later on we don't care about the file name at all)
            if '_streamDQM_' in json_file  or '_streamLookArea_' in json_file:
                streamDQM.append(json_file)
            elif '_streamDQMHistograms_' in json_file:
                streamDQMHistograms.append(json_file)
            elif '_streamDQMCalibration_' in json_file:
                streamDQMCalibration.append(json_file)
        streams = [sorted(streamDQM),
                   sorted(streamDQMHistograms),
                   sorted(streamDQMCalibration)]
        return streams

    @staticmethod
    def get_run_number_from_streams(streams):
        # We take the first file from the stream with most files:
        longest_stream = max(streams, key=len)
        first_file_name = longest_stream[0]
        run_number = FFFSimulator.get_run_number_from_file_name(first_file_name)
        if not run_number:
            logging.error('Couldn\'t determine run number from %s (the first '
                          'file in the longest stream).' % first_file_name)
        return run_number

    @staticmethod
    def get_run_number_from_file_name(file_name):
        if re.search('(run)(\d+)', file_name):
            return int(re.search('(run)(\d+)', file_name).group(2))

    @staticmethod
    def start_simulating(source_run, source_dir, ramdisk_dir, run_key,
                         seconds_per_lumi):
        # First get what we have as streams from the source (input) data.
        # We basically need to simulate 3 new streams, given this source data.
        streams = FFFSimulator.get_streams(source_run, source_dir)
        # We decide on the initial run number of the run we will simulate:
        run_number = FFFSimulator.get_run_number_from_streams(streams)
        # We need to decide how many lumi sections to simulate, we take the
        # stream with the highest number of files and use that number:
        lumi_amount = max([len(stream) for stream in streams])
        # We will also skip the first 2 lumisections...
        # ...and leave "a hole" of 3 lumisections after the 10th
        lumis_to_skip = [1, 2, 11, 12, 13]
        # So the total length of the run we can simulate is a bit longer:
        lumi_amount += len(lumis_to_skip)
        # To make sure we actually simulate the gap, we ensure that the minimum
        # run lenght is always 15, even if the amount of input files is too
        # small.
        lumi_amount = max(lumi_amount, 15)
        # We start simulation runs forever:
        while True:
            logging.info('Starting simulation of new run %d.' % run_number)
            FFFSimulator.simulate_run(run_number, lumi_amount, lumis_to_skip,
                               streams, ramdisk_dir, run_key, seconds_per_lumi)
            # After the run we increment the run number for the next run to
            # simulate:
            run_number += 1
        pass

    @staticmethod
    def simulate_run(run_number, lumi_amount, lumis_to_skip,
                     streams, ramdisk_dir, run_key, seconds_per_lumi):
        # A. We Start the run:
        FFFSimulator.create_global_file(run_number, ramdisk_dir, run_key)
        FFFSimulator.create_run_directory(run_number, ramdisk_dir)
        # B. We loop over all the lumisections we simulate:
        for lumi_number in range(1, lumi_amount + 1):
            logging.info('Simulating run %s, lumisection %s' % (run_number,
                                                                lumi_number))
            if lumi_number in lumis_to_skip:
                logging.info('  Skipping this lumisection on purpose.')
                # If we're skipping the lumis, we sleep a bit less long:
                time.sleep(2)
            else:
                FFFSimulator.simulate_lumisection(run_number, lumi_number,
                                                  streams, ramdisk_dir)
                time.sleep(seconds_per_lumi)
        # C. We finalize the run:
        FFFSimulator.write_EoR_file(run_number, ramdisk_dir)

    @staticmethod
    def simulate_lumisection(run_number, lumi_number, streams, ramdisk_dir):
        FFFSimulator.simulate_stream(run_number, lumi_number,
                                     'streamDQM',
                                     streams[0], ramdisk_dir)
        FFFSimulator.simulate_stream(run_number, lumi_number,
                                     'streamDQMHistograms',
                                     streams[1], ramdisk_dir)
        FFFSimulator.simulate_stream(run_number, lumi_number,
                                     'streamDQMCalibration',
                                     streams[2], ramdisk_dir)

    @staticmethod
    def simulate_stream(run_number, lumi_number, stream_name, stream,
                        ramdisk_dir):
        logging.info('  For %s:' % stream_name)

        # The input - We always have a json file and a data file
        # We take the first file in the stream
        input_json_full_path = stream[0]
        input_dir = os.path.dirname(input_json_full_path)
        json_info = FFFSimulator.get_info_from_json(input_json_full_path)
        input_data_name = json_info['data'][3]
        input_data_full_path = os.path.join(input_dir, input_data_name)
        input_data_extention = os.path.splitext(input_data_name)[1]

        # The output - We again have a json file and a data file
        output_base_name = FFFSimulator.format_base_name(run_number,
                                                   lumi_number, stream_name)
        output_json_name = output_base_name + '.jsn'
        output_data_name = output_base_name + input_data_extention
        output_dir = FFFSimulator.format_dir_name(run_number, ramdisk_dir)
        output_json_full_path = os.path.join(output_dir, output_json_name)
        output_data_full_path = os.path.join(output_dir, output_data_name)

        # Modify the data file path in the json info:
        json_info['data'][3] = output_data_name

        # Write the output:
        FFFSimulator.copy_data(input_data_full_path, output_data_full_path)
        FFFSimulator.copy_json(input_json_full_path, output_json_full_path,
                                                                    json_info)

        # Rotate the stream list (in place): First element goes to the end.
        stream.append(stream.pop(0))

    @staticmethod
    def get_info_from_json(input_json_full_path):
        with open(input_json_full_path, 'r') as input_json_file:
            json_data = json.load(input_json_file)
        return json_data

    @staticmethod
    def copy_json(input_json_full_path, output_json_full_path, json_info):
        with open(output_json_full_path, 'w') as output_json_file:
            output_json_file.write(json.dumps(json_info))
        logging.info('    Json:    %s' % input_json_full_path)
        logging.info('    Becomes: %s' % output_json_full_path)

    @staticmethod
    def copy_data(input_data_full_path, output_data_full_path):
        shutil.copyfile(input_data_full_path, output_data_full_path)
        logging.info('    Data:    %s' % input_data_full_path)
        logging.info('    Becomes: %s' % output_data_full_path)

    @staticmethod
    def format_base_name(run_number, lumi_number, stream_name):
        # Method intended to format the output file name for the file that
        # we will eventually put on the ramdisk - without extension
        run_part = 'run%d' % run_number
        lumi_part = 'ls%04d' % lumi_number
        return '%s_%s_%s_mrg-FFFSimulator' % (run_part, lumi_part, stream_name)

    @staticmethod
    def format_dir_name(run_number, ramdisk_dir):
        run_dir = 'run%d' % run_number
        return os.path.join(ramdisk_dir, run_dir)

    @staticmethod
    def create_global_file(run_number, ramdisk_dir, run_key):
        # Creates the hidden .run*.global run file on the ramdisk.
        file_name = '.run%d.global' % run_number
        full_name = os.path.join(ramdisk_dir, file_name)
        # No need to check if it already exists, since we cleaned first.
        with open(full_name, 'w') as global_file:
            global_file.write('run_key = %s' % run_key)
        logging.info('Created hidden .global run file %s' % full_name)

    @staticmethod
    def create_run_directory(run_number, ramdisk_dir):
        dir_name = FFFSimulator.format_dir_name(run_number, ramdisk_dir)
        # No need to check if it already exists, since we cleaned first.
        os.makedirs(dir_name, 0755)
        logging.info('Created run directory: %s' % dir_name)
        # Now the famous Atanas hack to give inotify time to work correctly
        time.sleep(1)

    @staticmethod
    def write_EoR_file(run_number, ramdisk_dir):
        file_name = 'run%d_ls0000_EoR.jsn' % run_number
        dir_name = FFFSimulator.format_dir_name(run_number, ramdisk_dir)
        full_name = os.path.join(dir_name, file_name)
        open(full_name, 'a').close()
        logging.info('Wrote EoR (end of run) file: %s' % full_name)

    def halt(self):
        # Load config variables to local variables for easy usage
        cfg = FFFSimulator.load_configuration()
        # To stop processing, we try to write an End-of-Run file
        logging.info("Simulator received request to stop.")
        fff_os_operations.hltd_stop()
        fff_os_operations.hltd_stop(cfg.fu_host_name)
        fff_os_operations.hltd_start()
        fff_os_operations.hltd_start(cfg.fu_host_name)

    @staticmethod
    def load_configuration():
        config = ConfigParser.RawConfigParser()
        config.read(CONFIGURATION_FILE)
        # We like to simplify things by promoting all settings to local
        # variables:
        config.source_run = config.get('General', 'SourceRun')
        config.source_dir = config.get('General', 'SourceDir')
        config.alternative_source_dirs = config.get('General',
                                                    'AlternativeSourceDirs')
        config.ramdisk_dir = config.get('General', 'RamdiskDir')
        config.fu_data_dir = config.get('General', 'FUDataDir')
        config.fu_host_name = config.get('General', 'FUHostName')
        config.run_key = config.get('General', 'RunKey')
        config.seconds_per_lumi = config.getfloat('General', 'SecondsPerLumi')
        return config

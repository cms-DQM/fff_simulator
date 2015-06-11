# This library was written to make abstraction of certain operations we would
# perform on the os level.
# Each operation also handles the parsing of the result, so that we don't have
# to worry about it in the calling code.

import os
import logging
from subprocess import Popen, PIPE

def hltd_status(host=None):
    command = ['service', 'hltd', 'status']
    if host:
        command[:0] = ['ssh', host]
    process = Popen(command, stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()
    if stderr:
        raise Exception(stderr)
    return stdout

def hltd_running(host=None):
    return 'hltd is running' in hltd_status(host)

def hltd_stop(host=None):
    command = ['service', 'hltd', 'stop']
    target = ' locally' # For logging
    if host:
        command[:0] = ['ssh', host]
        target = ' on %s' % host # For logging
    process = Popen(command, stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()
    # Output should be:
    # If stopped successfully:
    # Stopping hltd instance main: o.o.          [  OK  ]
    # Or if it was already stopped:
    # Stopping hltd instance main: not running, no pidfile /var/run/hltd.pid
    if 'OK' in stdout:
        logging.info('Stopped hltd%s' % target)
        return True
    if 'not running' in stdout:
        logging.info('Tried to stop hltd%s, but it was already stopped.'
                     % target)
        return True
    # If we didn't get the expected results, we raise an exception
    raise Exception('\n'.join([stdout, stderr]))

def hltd_start(host=None):
    command = ['service', 'hltd', 'start']
    target = ' locally' # For logging
    if host:
        command[:0] = ['ssh', host]
        target = ' on %s' % host # For logging
    process = Popen(command, stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()
    # Output should be:
    # If started successfully:
    # Starting hltd instance main :              [  OK  ]
    # Or if it was already started:
    # pidfile /var/run/hltd.pid already exists. Daemon already running?
    # (This second one in stderr, though.)
    if 'OK' in stdout:
        logging.info('Started hltd%s' % target)
        return True
    if 'already running' in stderr:
        logging.info('Tried to start hltd%s, but it was already started.'
                     % target)
        return True
    # If we didn't get the expected results, we raise an exception
    raise Exception('\n'.join([stdout, stderr]))

def execute_literal_shell_command(command):
    # Using direct shell interpretation here. Don't like it, but given the
    # non-functional constraints, this is best now.
    process = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()
    if stderr:
        raise Exception(stderr)

def clean_ramdisk(ramdisk_dir):
    # Deletes the old run* directories from the ramdisk.
    logging.info('Deleting old runs from ramdisk.')
    # We implement this in a very simple way, by just passing the rm
    # command to the os.
    runs_path = os.path.join(ramdisk_dir, 'run{,[0-9]}'+'[0-9]'*6)
    keys_path = os.path.join(ramdisk_dir, '.run{,[0-9]}'+'[0-9]'*6+'.global')
    command = 'rm -rf %s %s' % (runs_path, keys_path)
    execute_literal_shell_command(command)

def clean_fu_data_dir(fu_host_name, fu_data_dir):
    # Deletes the old run* directories from the ramdisk.
    logging.info('Deleting old runs from fu data dir.')
    # We implement this in a very simple way, by just passing the rm
    # command to the os.
    runs_path = os.path.join(fu_data_dir, 'run{,[0-9]}'+'[0-9]'*6)
    command = 'ssh %s rm -rf %s' % (fu_host_name, runs_path)
    execute_literal_shell_command(command)

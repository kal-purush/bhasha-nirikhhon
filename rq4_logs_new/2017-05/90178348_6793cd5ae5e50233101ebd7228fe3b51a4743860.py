#!/usr/bin/python3
""" main.py:
    Main entry-point into the run records import application.
"""

# Import Required Libraries (Standard, Third Party, Local) ********************
import asyncio
import csv
import datetime
import logging
import typing
import run_records

# Authorship Info *************************************************************
__author__ = "Christopher Maue"
__copyright__ = "Copyright 2017, The RPi-Home Project"
__credits__ = ["Christopher Maue"]
__license__ = "GPL"
__version__ = "1.0.0"
__maintainer__ = "Christopher Maue"
__email__ = "csmaue@gmail.com"
__status__ = "Development"


# Main event loop function ****************************************************
def main():
    """ main function for the run records import application """

    # Get configuration from INI file for this run
    logger, database, runmeter, tomtom = (
        run_records.configure_all('run_records//config.ini'))
    logger.info(
        'Run Records Application Started @ [%s] ******************************',
        str(datetime.datetime.now()))
    logger.info('Configuration imported from INI file')

    # Get main event loop *****************************************************
    logger.info('Getting main event loop')
    event_loop = asyncio.get_event_loop()

    # Run event loop until keyboard interrupt received ************************
    try:
        logger.info('Call run_until_complete on task list')
        event_loop.run_until_complete(
            asyncio.gather(
                #rpihome_v3.update_schedule(cal_credentials, True, logger),
                run_records.import_runmeter_records(runmeter, logger),
                run_records.import_tomtom_records(tomtom, logger)
                ))
        logger.info('Tasks are started')
    except KeyboardInterrupt:
        logger.debug('Closing connection to database')
        database.close()
        pass
    finally:
        logger.info(
            'Main event loop terminated @ [%s] ******************************',
            str(datetime.datetime.now()))
        event_loop.close()


# Call as script if run as __main__ *******************************************
if __name__ == '__main__':
    main()
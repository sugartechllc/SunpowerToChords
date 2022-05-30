import pandas
import argparse
import sys
import logging
import os
import datetime
import zoneinfo
import json
import pychords.tochords as tochords
import time


def stringToUnixTimestamp(datetimeString, year, tzinfo):
    """
    Convert a SunPower datetime string to a Unix timestamp.
    """

    # First try to parse as an iso string
    try:
        dt = datetime.datetime.fromisoformat(datetimeString)
        dt = dt.replace(tzinfo=tzinfo)
        return dt.timestamp()
    except Exception as e:
        pass

    # Next try to parse in Sunpower Datetime format (Saturday, 2/12 - 7:00am - 8:00am for example)
    try:
        # Split out end and startdt times
        items = datetimeString.split("-")
        start = items[0] + items[1].strip()
        end = items[0] + items[2].strip()

        # Convert start/end to datetime (old files are missing year...)
        startdt = datetime.datetime.strptime(start, "%A, %m/%d/%Y %I:%M%p")
        enddt = datetime.datetime.strptime(end, "%A, %m/%d/%Y %I:%M%p")
        startdt = startdt.replace(tzinfo=tzinfo)
        enddt = enddt.replace(tzinfo=tzinfo)

        # If the end is before the start, assume the day has wrapped around
        if enddt < startdt:
            enddt = enddt + datetime.timedelta(days=1)

        # Find the middle of the two times
        dt = startdt + ((enddt - startdt) / 2)
        return dt.timestamp()
    except Exception as e:
        logging.error(f"Failed to parse {datetimeString}")
        raise e


def readSunpowerReport(data_filepath, year=datetime.datetime.now().year, tzinfo=zoneinfo.ZoneInfo('US/Pacific')):
    """
    Read a sunpower xlsx file into a pandas data frame and convert timestamps into unix timestamps.
    """

    # Make sure the file exists
    if os.path.exists(data_filepath) == False:
        logging.error(f"{data_filepath} does not exist.")
        return None

    # Parse the file
    logging.info(f"Parsing {data_filepath}")
    dataframe = pandas.read_excel(data_filepath)
    if dataframe is None:
        logging.error(f"Failed to parse {data_filepath}")
        return None

    # Convert period to a unix timestamp
    if dataframe["Period"] is None:
        logging.error(
            f"{data_filepath} is does not contain a Period column, can not parse")
        return None
    dataframe["Unix Timestamp"] = dataframe["Period"].apply(
        stringToUnixTimestamp, args=(year, tzinfo,))

    return dataframe


def handleFile(config, file):
    """
    Handle a sunpower xlsx report file and send to chords.
    """

    # Parse the sunpower xlsx into a dataframe
    dataframe = readSunpowerReport(file)
    if dataframe is None:
        logging.error(f"Failed to parse {file}")
        sys.exit(-1)
    logging.debug("Data is:\n" + str(dataframe))
    logging.debug("Headers are:\n" + str(list(dataframe.columns)))

    # Loop through each column and see if it is a known variable
    for (col_name, col_data) in dataframe.iteritems():
        for var in config["variables"]:
            if col_name == var["column_name"]:
                timestamps = dataframe["Unix Timestamp"]
                sendData(config, var["short_name"], timestamps, col_data)


def sendData(config, short_name, timestamps, data):
    """
    Send sunpower data to chords. Takes two columns: unix timestamp and the data itself.
    """

    # Build and send the URI
    for timestamp, val in zip(timestamps, data):
        chords_record = {}
        chords_record["inst_id"] = config["instrument_id"]
        chords_record["api_email"] = config["api_email"]
        chords_record["api_key"] = config["api_key"]
        chords_record["vars"] = {}
        chords_record["vars"]["at"] = int(timestamp)
        chords_record["vars"][short_name] = val
        uri = tochords.buildURI(config["chords_host"], chords_record)
        logging.info(f"Submitting: {uri}")
        max_queue_length = 10*60*24
        tochords.submitURI(uri, max_queue_length)
        time.sleep(0.2)


def main(files, config_file):

    # Load configuration
    logging.info(f"Starting SunPower to Chords with {config_file}")
    config = json.loads(open(config_file).read())

    # Startup chords sender
    tochords.startSender()

    # Parse each sunpower xlsx file
    for file in files:
        logging.info(f"Handling: {file}")
        handleFile(config, file)

    # Wait for all data to be sent
    while True:
        num_remaining = tochords.waiting()
        logging.info(f"Queue length: {num_remaining}")
        time.sleep(1)
        if num_remaining == 0:
            break


if __name__ == '__main__':

    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-f', '--files', help='The files to compare.', required=True, nargs='+')
    parser.add_argument(
        "-c", "--config", help="Path to json configuration file to use.", required=True)
    parser.add_argument(
        "--debug", help="Enable debug logging",
        action="store_true")
    args = parser.parse_args()

    # Configure logging
    level = logging.INFO
    if args.debug:
        level = logging.DEBUG
    logging.basicConfig(stream=sys.stdout, level=level, format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    logging.debug("Debug logging enabled")

    # Run main
    main(args.files, args.config)

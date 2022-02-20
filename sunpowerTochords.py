import pandas
import argparse
import sys
import logging
import os
import datetime
import zoneinfo


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

        # Convert start/end to datetime
        startdt = datetime.datetime.strptime(start, "%A, %m/%d %I:%M%p")
        enddt = datetime.datetime.strptime(end, "%A, %m/%d %I:%M%p")
        startdt = startdt.replace(year=year)
        enddt = enddt.replace(year=year)
        startdt = startdt.replace(tzinfo=tzinfo)
        enddt = enddt.replace(tzinfo=tzinfo)

        # If the end is before the start, assume the day has wrapped around
        if enddt < startdt:
            enddt = enddt + datetime.timedelta(days=1)

        # Find the middle of the two times
        dt = startdt + ((enddt - startdt) / 2)
        return dt.timestamp()
    except Exception as e:
        print(e)
        pass

    logging.error(f"Failed to parse {datetimeString}")
    return 0


def readSunpowerReport(filepath, year=datetime.datetime.now().year, tzinfo=zoneinfo.ZoneInfo('US/Pacific')):
    """
    Read a sunpower xlsx file into a pandas data frame and convert timestamps into unix timestamps.
    """
    # Make sure the file exists
    if os.path.exists(filepath) == False:
        logging.error(f"{filepath} does not exist.")
        return None

    # Parse the file
    logging.info(f"Parsing {filepath}")
    dataframe = pandas.read_excel(filepath)
    if dataframe is None:
        logging.error(f"Failed to parse {filepath}")
        return None

    # Convert period to a unix timestamp
    if dataframe["Period"] is None:
        logging.error(
            f"{filepath} is does not contain a Period column, can not parse")
        return None
    dataframe["Unix Timestamp"] = dataframe["Period"].apply(
        stringToUnixTimestamp, args=(year, tzinfo,))

    return dataframe


def main(filepath):
    # Parse sunpower xlsx file
    dataframe = readSunpowerReport(filepath)
    if dataframe is None:
        logging.error(f"Failed to parse {filepath}")
        sys.exit(-1)

    logging.info("Data is:\n" + str(dataframe))


if __name__ == '__main__':

    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "filepath", help="Path to the sunpower xlsx report to parse.")
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
    main(args.filepath)

import argparse
import logging
from os import path
import sys
import datetime
import random
import json
from db.data import InterProData
from urllib.request import urlopen
from jsonschema import validate

logger = logging.getLogger(__name__)

def parseArguments(sysArgs):
    """Parses arguments and returns argument object"""
    parser = argparse.ArgumentParser(description="InterPro 7 data dumper: For EBI search")
    parser.add_argument("-c", "--config",
                        type=argparse.FileType('r'),
                        help="path to config file",
                        required=True,
                        metavar="configfile")
    parser.add_argument("-o", "--output",
                        type=argparse.FileType('w'),
                        help="output JSON file",
                        default="ebisearch.json",
                        metavar="configfile")
    parser.add_argument("-t", "--test",
                        default=0,
                        help="selects a number of random identifiers for testing output",
                        metavar="integer")
    parser.add_argument("-n", "--nocache",
                        action="store_true",
                        default=False,
                        help="do not use cache")
    parser.add_argument("-l", "--log",
                        action="store_true",
                        default=False,
                        help="log to stderr (as well as any file specified in the config)")
    parser.add_argument("-v", "--validate",
                        action="store_true",
                        default=False,
                        help="test that data will be converted to json")
    parser.add_argument("--verbose",
                        action="store_true",
                        default=False,
                        help="This option increases the level of logging output to 'debug' level")

    args = parser.parse_args(sysArgs)
    return args

def prepareLogger(args):
    """Prepares logging levels and output"""
    logFormat = "%(asctime)s %(name)s: %(funcName)s: %(levelname)s: %(message)s"
    logLevel = logging.WARN
    if args.verbose:
        logLevel = logging.INFO

    try:
        logfile = "interpro7-ebisearch.log"
        logging.basicConfig(level=logLevel, format=logFormat, filename=logfile, filemode="w")
        if args.log:
            streamLogger = logging.StreamHandler()
            streamLogger.setLevel(logLevel)
            streamLogger.setFormatter(logging.Formatter(logFormat))
            logging.getLogger('').addHandler(streamLogger)
    except KeyError:
        logging.basicConfig(level=logLevel, format=logFormat, stream=sys.stderr)

def main():
    args = parseArguments(sys.argv[1:])
    config = json.load(args.config)
    prepareLogger(args)

    #cache the json schema file
    if 'jsonschemaFile' in config and path.exists(config['jsonschemaFile']) :
        file = open(config['jsonschemaFile'], "r")
        data = file.read()
    else:
        url = config['jsonschemaURL']
        response = urlopen(url)
        data = response.read().decode('utf-8')
        if 'jsonschemaFile' in config:
            file = open(config['jsonschemaFile'], "w")
            file.write(data)
    searchSchema = json.loads(data)

    limit = None
    if int(args.test) > 0:
        limit = int(args.test)

    db = InterProData(config)
    results = db.getResults(nocache=args.nocache)
    entryDict = db.resultsToEntries(results, limit)

    if args.validate:
        try:
            validate(entryDict, searchSchema)
        except Exception as e:
            logger.error(e)
    args.output.write(json.dumps(entryDict, indent=4, sort_keys=True))

if __name__ == '__main__':
    scriptPath = path.dirname(path.abspath(__file__))
    sys.path.append(scriptPath)
    main()

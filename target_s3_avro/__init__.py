#!/usr/bin/env python3

import argparse
import avro.schema
import boto3
import collections
import dateutil.parser
import http.client
import io
import json
import os
import pkg_resources
import re
import singer
import sys
import tempfile
import threading
import urllib
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from datetime import datetime
from json import dumps, dump
from jsonschema.validators import Draft4Validator
from botocore.exceptions import ClientError


logger = singer.get_logger()


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


def flatten(d, parent_key='', flatten_delimiter='__'):
    items = []
    for k, v in d.items():
        new_key = parent_key + flatten_delimiter + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, flatten_delimiter=flatten_delimiter).items())
        else:
            items.append((new_key, str(v) if type(v) is list else v))
    return dict(items)


def _flatten_avsc(a, parent_key='', flatten_delimiter='__'):
    field_list = []
    dates_list = []
    type_switcher = {"integer": "int",
                     "number": "double",
                     "date-time": "long"}
    default_switcher = {"integer": 0,
                        "number": 0.0,
                        "date-time": 0}

    for k, v in a.items():
        if (v.get("selected") == "true" or v.get("inclusion") == "automatic" or parent_key)\
                and v.get("inclusion") != "unsupported":
            append_default_element = False
            new_key = parent_key + flatten_delimiter + k if parent_key else k
            type_list = ["null"]
            default_val = None
            types = [v.get("type")] if isinstance(v.get("type"), str) else v.get("type")
            # Convert legacy "anyOf" field types to string & check for date-time format
            if v.get("type") is None and v.get("anyOf"):
                for ao_iter in v.get("anyOf"):
                    if ao_iter.get("format") == "date-time":
                        v["format"] = "date-time"
                types = ["null", "string"]
            for t in types:
                if t == "object" or t == "dict":
                    recurs_avsc, recurs_dates = _flatten_avsc(v["properties"],
                                                              parent_key=new_key,
                                                              flatten_delimiter=flatten_delimiter)
                    field_list.extend(recurs_avsc)
                    dates_list.extend(recurs_dates)
                    append_default_element = True
                elif t == "array":
                    type_list.append(type_switcher.get("string", "string"))
                    default_val = default_switcher.get("string", None)
                elif t == "string" and v.get("format") == "date-time":
                    dates_list.append(new_key)
                    type_list.append(type_switcher.get("date-time", t))
                    default_val = default_switcher.get("date-time", None)
                elif t == "null":
                    pass
                else:
                    type_list.append(type_switcher.get(t, t))
                    default_val = default_switcher.get(t, None)

            if append_default_element:
                new_element = {"name": new_key, "type": ["null", "string"], "default": None}
            else:
                new_element = {"name": new_key, "type": type_list, "default": default_val}

            # Handle all disallowed avro characters in the field name with alias
            pattern = r"[^A-Za-z0-9_]"
            if re.search(pattern, k):
                new_element["alias"] = new_key
                new_element["name"] = re.sub(pattern, "_", k)

            field_list.append(new_element)

    return list(field_list), list(dates_list)


def persist_lines(config, lines):
    state = None
    schemas = {}
    key_properties = {}
    validators = {}
    avsc_basename = {}
    avsc_files = {}
    avro_files = {}
    schema_date_fields = {}

    now = ("-" + datetime.now().strftime('%Y%m%dT%H%M%S')) if config.get("include_timestamp") != "false" else ""
    flatten_delimiter = config.get("flatten_delimiter", "__")

    logger.info('Connecting to s3 ...')
    s3_client = boto3.client(
        service_name='s3',
        region_name=config.get("region_name"),
        api_version=config.get("api_version"),
        use_ssl=config.get("use_ssl"),
        verify=config.get("verify"),
        endpoint_url=config.get("endpoint_url"),
        aws_access_key_id=config.get("aws_access_key_id"),
        aws_secret_access_key=config.get("aws_secret_access_key"),
        aws_session_token=config.get("aws_session_token"),
        config=config.get("config")
    )
    tdir = config.get("tmp_dir", os.getcwd())

    if tdir:
        if not os.path.isdir(tdir):
            raise Exception("Path '{0}' from config.tmp_dir does not exist!".format(tdir))

    bucket_prefix_regex = re.compile(r'.*(?<!:)$')

    logger.info('Validating target_bucket_key: {} ...'.format(config.get("target_bucket_key")))
    # Remove empty strings and any s3 Prefix defined in the target_location
    target_location = list(filter(bucket_prefix_regex.match, filter(None, config.get("target_bucket_key").split("/"))))
    # Use first element in the target_location as the target_bucket
    target_bucket = target_location[0]
    # Use all elements except the last as the target_key
    target_key = "/".join(target_location[1:])

    # Check if the s3 bucket exists
    try:
        s3_client.head_bucket(Bucket=target_bucket)
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == '404':
            raise Exception("Bucket {0} does not exist!".format(target_bucket))

    # if no target_schema_bucket_key is passed, use the target_bucket_key
    if config.get("target_schema_bucket_key") is None:
        logger.info('No target_schema_bucket_key passed. Using target_bucket_key: {} ...'
                    .format(config.get("target_bucket_key")))
        target_schema_bucket = target_bucket
        target_schema_key = target_key
    else:
        logger.info('Validating target_schema_bucket_key: {} ...'.format(config.get("target_schema_bucket_key")))
        # Remove empty strings and any s3 Prefix defined in the target_schema_location
        target_schema_location = list(filter(bucket_prefix_regex.match,
                                             filter(None, config.get("target_schema_bucket_key").split("/"))))
        # Use first element in the target_schema_location as the target_schema_bucket
        target_schema_bucket = target_schema_location[0]
        # Use all elements except the last as the target_schema_key
        target_schema_key = "/".join(target_schema_location[1:])

        # Check if the s3 bucket exists
        try:
            s3_client.head_bucket(Bucket=target_schema_bucket)
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == '404':
                raise Exception("Bucket {0} does not exist!".format(target_schema_bucket))

    logger.info('Processing input ...')
    # create temp directory for processing
    with tempfile.TemporaryDirectory(dir=tdir) as temp_dir:
        # Loop over lines from stdin
        for line in lines:
            try:
                o = json.loads(line)
            except json.decoder.JSONDecodeError:
                logger.info(line)
                logger.error("Unable to parse:\n{0}".format(line))
                raise

            if 'type' not in o:
                raise Exception("Line is missing required key 'type': {}".format(line))
            t = o['type']

            if t == 'RECORD':
                if 'stream' not in o:
                    raise Exception("Line is missing required key 'stream': {}".format(line))
                if o['stream'] not in schemas:
                    raise Exception("A record for stream {}".format(o['stream']) +
                                    " was encountered before a corresponding schema")

                # Validate record
                validators[o['stream']].validate(o['record'])

                # Convert date fields in the record
                for df_iter in schema_date_fields[o['stream']]:
                    if o['record'][df_iter] is not None:
                        dt_value = dateutil.parser.parse(o['record'][df_iter])
                        o['record'][df_iter] = int(dt_value.strftime("%s"))

                flattened_record = flatten(o['record'], flatten_delimiter=flatten_delimiter)

                # writing to a file for the stream
                avro_files[o['stream']].append(flattened_record)

                state = None
            elif t == 'STATE':
                logger.debug('Setting state to {}'.format(o['value']))
                state = o['value']
            elif t == 'SCHEMA':
                if 'stream' not in o:
                    raise Exception("Line is missing required key 'stream': {}".format(line))
                stream = o['stream']
                schemas[stream] = o['schema']
                validators[stream] = Draft4Validator(o['schema'])
                if 'key_properties' not in o:
                    raise Exception("key_properties field is required")
                key_properties[stream] = o['key_properties']
                avro_files[stream] = open(os.path.join(temp_dir, "{0}{1}.json".format(stream, now)), 'a')

                # read in the catalog and add selected fields to the avro schema
                schema_date_fields[stream] = []

                avsc_fields, schema_date_fields[stream] = _flatten_avsc(o['schema']["properties"],
                                                                        flatten_delimiter=flatten_delimiter)

                avsc_dict = {"namespace": "{0}.avro".format(stream),
                             "type": "record",
                             "name": "{0}".format(stream),
                             "fields": list(avsc_fields)}

                # write the avro schema out to a file
                avsc_basename[stream] = os.path.join(temp_dir, "{0}{1}".format(stream, now))
                avsc_files[stream] = open("{0}.avsc".format(avsc_basename[stream]), 'a')
                avsc_files[stream].truncate()
                dump(avsc_dict, avsc_files[stream], indent=2)
                avsc_files[stream].close()

                # open the avro data file
                avro_schema = avro.schema.Parse(dumps(avsc_dict))
                avro_files[stream] = DataFileWriter(open("{0}.avro".format(avsc_basename[stream]), "wb"),
                                                    DatumWriter(),
                                                    avro_schema)
            elif t == 'ACTIVATE_VERSION':
                logger.debug('Completed sync of {0} version {1}.'.format(o['stream'], o['version']))
            else:
                raise Exception("Unknown message type {} in message {}"
                                .format(o['type'], o))
        # Close all stream files and move to s3 target location
        for file_iter in avro_files.keys():
            avro_files[file_iter].close()
            try:
                file_name = "{0}.avro".format(avsc_basename[file_iter])
                logger.info('Moving file ({0}) to s3 location: {1}/{2} ...'.format(file_name,
                                                                                   target_bucket,
                                                                                   target_key))
                s3_client.upload_file(file_name,
                                      target_bucket,
                                      target_key + "/" + os.path.basename(file_name))

                file_name = "{0}.avsc".format(avsc_basename[file_iter])
                logger.info('Moving file ({0}) to s3 location: {1}/{2} ...'.format(file_name,
                                                                                   target_schema_bucket,
                                                                                   target_schema_key))
                s3_client.upload_file(file_name,
                                      target_schema_bucket,
                                      target_schema_key + "/" + os.path.basename(file_name))
            except ClientError as e:
                logger.error(e)

        

    return state


def send_usage_stats():
    try:
        version = pkg_resources.get_distribution('target-csv').version
        conn = http.client.HTTPConnection('collector.singer.io', timeout=10)
        conn.connect()
        params = {
            'e': 'se',
            'aid': 'singer',
            'se_ca': 'target-s3-avro',
            'se_ac': 'open',
            'se_la': version,
        }
        conn.request('GET', '/i?' + urllib.parse.urlencode(params))
        conn.getresponse()
        conn.close()
    except:
        logger.debug('Collection request failed')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()

    if args.config:
        with open(args.config) as args_input:
            config = json.load(args_input)
    else:
        config = {}

    # Validate required config settings
    if config.get("aws_access_key_id") is None:
        raise Exception("ERROR: 'aws_access_key_id' MUST be defined in config.")
    if config.get("aws_secret_access_key") is None:
        raise Exception("ERROR: 'aws_secret_access_key' MUST be defined in config.")
    if config.get("target_bucket_key") is None:
        raise Exception("ERROR: 'target_bucket_key' MUST be defined in config.")

    if not config.get('disable_collection', False):
        logger.info('Sending version information to singer.io. ' +
                    'To disable sending anonymous usage data, set ' +
                    'the config parameter "disable_collection" to true')
        threading.Thread(target=send_usage_stats).start()

    std_input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = persist_lines(config, std_input)

    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()

import sys
import os
import json
import itertools
import logging
import random

import rds_config
import pymysql
import base64
import boto3


#rds settings
rds_host  = rds_config.db_host
name = rds_config.db_username
password = rds_config.db_password
db_name = rds_config.db_name

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DRY_RUN = (os.getenv('DRY_RUN', 'false') == 'true')

AWS_REGION = os.getenv('REGION_NAME', 'ap-northeast-2')
KINESIS_STREAM_NAME = os.getenv('KINESIS_STREAM_NAME', 'kinesis_stream')

REQUIRED_FIELDS = [e for e in os.getenv('REQUIRED_FIELDS', '').split(',') if e]

random.seed(47)

try:
    conn = pymysql.connect(rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
except pymysql.MySQLError as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    sys.exit()
    
def put_records_to_kinesis(client, stream_name, record):
  MAX_RETRY_COUNT = 3

  payload_list = []

  partition_key = 'part-{:05}'.format(random.randint(1, 1024))
  payload_list.append({'Data': record, 'PartitionKey': partition_key})

  if DRY_RUN:
    print(json.dumps(payload_list, ensure_ascii=False))
    return

  for _ in range(MAX_RETRY_COUNT):
    try:
      response = client.put_records(Records=payload_list, StreamName=stream_name)
      break
    except Exception as ex:
      logger.error(ex)
  else:
    raise RuntimeError('[ERROR] Failed to put_records into stream: {}'.format(stream_name))


def put_records_to_firehose(client, stream_name, record):
  MAX_RETRY_COUNT = 3

  if DRY_RUN:
    print(record)
  else:
    for _ in range(MAX_RETRY_COUNT):
      try:
        response = client.put_record(
          DeliveryStreamName=stream_name,
          Record={
            'Data': '{}\n'.format(record)
          }
        )
        break
      except Exception as ex:
        logger.error(ex)
    else:
      raise RuntimeError('[ERROR] Failed to put_records into stream: {}'.format(stream_name))


def lambda_handler(event, context):
    import collections
    kinesis_client = boto3.client('kinesis', region_name=AWS_REGION)
    firehose_client = boto3.client('firehose', region_name=AWS_REGION)
    
    counter = collections.OrderedDict([('reads', 0),
        ('writes', 0),
        ('invalid', 0),
        ('index_errors', 0),
        ('errors', 0)])

    for record in event['Records']:
        try:
            counter['reads'] += 1
            payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            json_data = json.loads(payload)
    
            if not any([json_data.get(k, None) for k in REQUIRED_FIELDS]):
                counter['invalid'] += 1
                continue

            with conn.cursor() as cur:
                cur.execute("select birth_dt, gender_cd from users where swid='" + json_data["user_session_id"]+"'")
                logger.info("selected users item size : " + str(cur.rowcount))
                for row in cur:
                    logger.info(row)
                    json_data["birth_dt"] = row[0]
                    json_data["gender_cd"] = row[1]
                    
                url = json_data["url"].replace("http://www.RL.com", "")
                cur.execute("select url, category, id from products where url='" + url +"'")
                logger.info("selected products item size : " + str(cur.rowcount))
                for row in cur:
                    logger.info(row)
                    json_data["category"] = row[0]
                    json_data["id"] = row[1]
                    
                logger.info(json_data)
                # put_records_to_kinesis(kinesis_client, KINESIS_STREAM_NAME, json_data)
                put_records_to_firehose(firehose_client, KINESIS_STREAM_NAME, json_data)
                conn.commit()
        except Exception as ex:
            import time
    
            logger.error(ex)
            time.sleep(2)
        # finally:
        #     conn.close()



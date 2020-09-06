import json
from os import getenv
from datetime import datetime
from dateutil.relativedelta import relativedelta
foo_table_name = getenv('foo_table')


def lambda_handler(event, context):
    response, count = query_table(foo_table_name, "primary_key", primary_key=0)

    return response


def cherry_pick_keys(new_image) :
    L_to_R_columns = {
      "fls_id": "fls_id",
      "lead_row_id": "lead_row_id",
      "meeting_created_datetime": "meeting_created_datetime",
      "meeting_datetime": "meeting_datetime",
      "meeting_status": "meeting_status"
    }
    insert_dict = {}
    # take new_image and produce a dictionary
    for k,v in new_image.items():
        if k in L_to_R_columns:
            new_key = L_to_R_columns[k]
            insert_dict[new_key] = list(v.items())[0][1]
    # logic to append  update_dttm
    insert_dict['updated_dttm'] =  (datetime.now() + relativedelta(hours=5,minutes=30)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    return insert_dict


def query_table(foo_table_str, primary_key_attribute, primary_key, secondary_key_attribute=False, secondary_key=False, gsi=False, skey=False):
    import boto3
    dyndb = boto3.resource('dynamodb')
    from boto3.dynamodb.conditions import Key, Attr  # 2 required

    foo_table = dyndb.Table(foo_table_str)
    if gsi:             # FOR GSI
        response = foo_table.query(IndexName=primary_key_attribute,
                                   KeyConditionExpression=Key(primary_key_attribute.split("-")[0]).eq(primary_key))
    elif skey:          # FOR PK SK
        response = foo_table.query(KeyConditionExpression=Key(primary_key_attribute).eq(primary_key) & Key(secondary_key_attribute).eq(secondary_key))
    else:               # FOR PK
        response = foo_table.query(KeyConditionExpression=Key(primary_key_attribute).eq(primary_key))

    response_items = response.get('Items', [])
    count_items = response.get("Count")

    return response_items, count_items

import json
import boto3
from os import getenv
# from boto3.dynamodb.conditions import Key, Attr  # 2 required

dyndb = boto3.resource('dynamodb')
foo_table_name = getenv('foo_table')


def lambda_handler(event, context):
    response_items = scan_table(foo_table_name)

    return response_items


def scan_table(foo_table_str, var=0):
    foo_table = dyndb.Table(foo_table_str)

    # 1 simple scan
    response = foo_table.scan()

    # 2 scan with filtered attributes
    # var = "some_value"
    # foo_table.scan(FilterExpression = Attr('sub_channel').eq(var))

    # Get the Items / Records from response
    response_items = response.get('Items', [])



    return response_items


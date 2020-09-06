import json
import boto3
from os import getenv
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from boto3.dynamodb.conditions import Key, Attr  # 2 required

foo_table_str = getenv('foo_table')


def lambda_handler(event, context):
    current_dttm = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    res = update_table(foo_table_str, event.get("primary_value"), "New_key", "New_value")
    print("Update: ", res.get("HttpStatus"))
    return "Success"


def putItem(data, table_name):
    """

    :param dict data: must contain primary key (and sort_key if available)
    :param str table_name: table name must be in string, will be autoconverted to dydb object
    :return:
    """
    dyndb = boto3.resource('dynamodb')
    foo_table = dyndb.Table(table_name)
    # Latest updated timestamp of this lambda
    data["updated_dttm"] = (datetime.now() + timedelta(hours=5, minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ")

    for i in range(10):
        response = foo_table.put_item(
            Item=data
        )
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return True
        else:
            return False


def customizeBatchWrite(insertDictWithBatch, table_name, pk, sk):
    dyndb = boto3.resource('dynamodb')
    import traceback

    insertRetryCount = 50
    for i in range(0, insertRetryCount):
        try:
            response = dyndb.batch_write_item(RequestItems=insertDictWithBatch)
        except Exception as e:
            traceback.print_exc()
            insertDictSet = set()
            for d in insertDictWithBatch[table_name]:
                vals_part = d['PutRequest']['Item'][pk]
                vals_sort = d['PutRequest']['Item'][sk]
                demoSet = (vals_part, vals_sort)
                if demoSet in insertDictSet:
                    insertDictWithBatch[table_name].remove(d)
                insertDictSet.add(demoSet)
            continue
        unprocessed = response.get('UnprocessedItems', None)

        if not unprocessed:
            return True
        unprocessed_list = unprocessed[table_name]
        insertDictWithBatch = {table_name: []}
        for u in unprocessed_list:
            insertDictWithBatch[table_name].append(u)

    return False


def update_table(foo_table_str, primary_value, set_key, set_value):
    """

    :param foo_table_str:
    :param primary_value:
    :param set_key:
    :param set_value:
    :return:
    """
    dyndb = boto3.resource('dynamodb')
    foo_table = dyndb.Table(foo_table_str)
    attempt = 0

    while attempt != 10:
        try:
            RESP = foo_table.update_item(
                Key={
                    'primary_key': primary_value
                },
                UpdateExpression=f"set {set_key} = :p",
                ExpressionAttributeValues={
                    ':p': json.dumps(set_value)
                },
                ReturnValues="UPDATED_NEW"
            )
            return RESP.get("ResponseMetadata")
        except Exception as e:
            print(f"ERR: Retrying to update item. Attempt {attempt + 1}. Error encountered {type(e)}")
            attempt += 1
    return "Exited"


def update_table_v1(final_dict, table_name_str, primary_key):
    dyndb = boto3.resource('dynamodb', 'ap-south-1')
    foo_table = dyndb.Table(table_name_str)
    attempt = 0

    DYNAMODB_RESERVED_WORDS = ['abort', 'absolute', 'action', 'add', 'after', 'agent', 'aggregate', 'all', 'allocate',
                               'alter', 'analyze', 'and', 'any', 'archive', 'are', 'array', 'as', 'asc', 'ascii',
                               'asensitive', 'assertion', 'asymmetric', 'at', 'atomic', 'attach', 'attribute', 'auth',
                               'authorization', 'authorize', 'auto', 'avg', 'back', 'backup', 'base', 'batch', 'before',
                               'begin', 'between', 'bigint', 'binary', 'bit', 'blob', 'block', 'boolean', 'both',
                               'breadth', 'bucket', 'bulk', 'by', 'byte', 'call', 'called', 'calling', 'capacity',
                               'cascade', 'cascaded', 'case', 'cast', 'catalog', 'char', 'character', 'check', 'class',
                               'clob', 'close', 'cluster', 'clustered', 'clustering', 'clusters', 'coalesce', 'collate',
                               'collation', 'collection', 'column', 'columns', 'combine', 'comment', 'commit',
                               'compact', 'compile', 'compress', 'condition', 'conflict', 'connect', 'connection',
                               'consistency', 'consistent', 'constraint', 'constraints', 'constructor', 'consumed',
                               'continue', 'convert', 'copy', 'corresponding', 'count', 'counter', 'create', 'cross',
                               'cube', 'current', 'cursor', 'cycle', 'data', 'database', 'date', 'datetime', 'day',
                               'deallocate', 'dec', 'decimal', 'declare', 'default', 'deferrable', 'deferred', 'define',
                               'defined', 'definition', 'delete', 'delimited', 'depth', 'deref', 'desc', 'describe',
                               'descriptor', 'detach', 'deterministic', 'diagnostics', 'directories', 'disable',
                               'disconnect', 'distinct', 'distribute', 'do', 'domain', 'double', 'drop', 'dump',
                               'duration', 'dynamic', 'each', 'element', 'else', 'elseif', 'empty', 'enable', 'end',
                               'equal', 'equals', 'error', 'escape', 'escaped', 'eval', 'evaluate', 'exceeded',
                               'except', 'exception', 'exceptions', 'exclusive', 'exec', 'execute', 'exists', 'exit',
                               'explain', 'explode', 'export', 'expression', 'extended', 'external', 'extract', 'fail',
                               'false', 'family', 'fetch', 'fields', 'file', 'filter', 'filtering', 'final', 'finish',
                               'first', 'fixed', 'flattern', 'float', 'for', 'force', 'foreign', 'format', 'forward',
                               'found', 'free', 'from', 'full', 'function', 'functions', 'general', 'generate', 'get',
                               'glob', 'global', 'go', 'goto', 'grant', 'greater', 'group', 'grouping', 'handler',
                               'hash', 'have', 'having', 'heap', 'hidden', 'hold', 'hour', 'identified', 'identity',
                               'if', 'ignore', 'immediate', 'import', 'in', 'including', 'inclusive', 'increment',
                               'incremental', 'index', 'indexed', 'indexes', 'indicator', 'infinite', 'initially',
                               'inline', 'inner', 'innter', 'inout', 'input', 'insensitive', 'insert', 'instead', 'int',
                               'integer', 'intersect', 'interval', 'into', 'invalidate', 'is', 'isolation', 'item',
                               'items', 'iterate', 'join', 'key', 'keys', 'lag', 'language', 'large', 'last', 'lateral',
                               'lead', 'leading', 'leave', 'left', 'length', 'less', 'level', 'like', 'limit',
                               'limited', 'lines', 'list', 'load', 'local', 'localtime', 'localtimestamp', 'location',
                               'locator', 'lock', 'locks', 'log', 'loged', 'long', 'loop', 'lower', 'map', 'match',
                               'materialized', 'max', 'maxlen', 'member', 'merge', 'method', 'metrics', 'min', 'minus',
                               'minute', 'missing', 'mod', 'mode', 'modifies', 'modify', 'module', 'month', 'multi',
                               'multiset', 'name', 'names', 'national', 'natural', 'nchar', 'nclob', 'new', 'next',
                               'no', 'none', 'not', 'null', 'nullif', 'number', 'numeric', 'object', 'of', 'offline',
                               'offset', 'old', 'on', 'online', 'only', 'opaque', 'open', 'operator', 'option', 'or',
                               'order', 'ordinality', 'other', 'others', 'out', 'outer', 'output', 'over', 'overlaps',
                               'override', 'owner', 'pad', 'parallel', 'parameter', 'parameters', 'partial',
                               'partition', 'partitioned', 'partitions', 'path', 'percent', 'percentile', 'permission',
                               'permissions', 'pipe', 'pipelined', 'plan', 'pool', 'position', 'precision', 'prepare',
                               'preserve', 'primary', 'prior', 'private', 'privileges', 'procedure', 'processed',
                               'project', 'projection', 'property', 'provisioning', 'public', 'put', 'query', 'quit',
                               'quorum', 'raise', 'random', 'range', 'rank', 'raw', 'read', 'reads', 'real', 'rebuild',
                               'record', 'recursive', 'reduce', 'ref', 'reference', 'references', 'referencing',
                               'regexp', 'region', 'reindex', 'relative', 'release', 'remainder', 'rename', 'repeat',
                               'replace', 'request', 'reset', 'resignal', 'resource', 'response', 'restore', 'restrict',
                               'result', 'return', 'returning', 'returns', 'reverse', 'revoke', 'right', 'role',
                               'roles', 'rollback', 'rollup', 'routine', 'row', 'rows', 'rule', 'rules', 'sample',
                               'satisfies', 'save', 'savepoint', 'scan', 'schema', 'scope', 'scroll', 'search',
                               'second', 'section', 'segment', 'segments', 'select', 'self', 'semi', 'sensitive',
                               'separate', 'sequence', 'serializable', 'session', 'set', 'sets', 'shard', 'share',
                               'shared', 'short', 'show', 'signal', 'similar', 'size', 'skewed', 'smallint', 'snapshot',
                               'some', 'source', 'space', 'spaces', 'sparse', 'specific', 'specifictype', 'split',
                               'sql', 'sqlcode', 'sqlerror', 'sqlexception', 'sqlstate', 'sqlwarning', 'start', 'state',
                               'static', 'status', 'storage', 'store', 'stored', 'stream', 'string', 'struct', 'style',
                               'sub', 'submultiset', 'subpartition', 'substring', 'subtype', 'sum', 'super',
                               'symmetric', 'synonym', 'system', 'table', 'tablesample', 'temp', 'temporary',
                               'terminated', 'text', 'than', 'then', 'throughput', 'time', 'timestamp', 'timezone',
                               'tinyint', 'to', 'token', 'total', 'touch', 'trailing', 'transaction', 'transform',
                               'translate', 'translation', 'treat', 'trigger', 'trim', 'true', 'truncate', 'ttl',
                               'tuple', 'type', 'under', 'undo', 'union', 'unique', 'unit', 'unknown', 'unlogged',
                               'unnest', 'unprocessed', 'unsigned', 'until', 'update', 'upper', 'url', 'usage', 'use',
                               'user', 'users', 'using', 'uuid', 'vacuum', 'value', 'valued', 'values', 'varchar',
                               'variable', 'variance', 'varint', 'varying', 'view', 'views', 'virtual', 'void', 'wait',
                               'when', 'whenever', 'where', 'while', 'window', 'with', 'within', 'without', 'work',
                               'wrapped', 'write', 'year', 'zone']

    for i in range(10):
        try:
            for dd in final_dict:

                # update_expr = expression_list[0]
                # attr_dict = expression_list[1]
                # attr_names = expression_list[2]

                dd['updated_dttm'] = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                float_val_str = {k: str(v) for k, v in dd.items() if isinstance(v, float)}
                dd.update(float_val_str)
                dd = {k: v for k, v in dd.items() if v not in [' ', '', None]}

                cnt = 0
                update_expr = "SET "
                attr_names = dict()
                attr_dict = dict()
                for k, v in dd.items():
                    if k not in [primary_key]:
                        if k.lower() in DYNAMODB_RESERVED_WORDS:
                            new_k = "#" + k[:-1]
                            attr_names.update({new_k: k})
                            k = new_k
                        update_expr = update_expr + " " + k + " = " + ":var" + str(cnt + 1) + ", "
                        ka = ':var' + str(cnt + 1)
                        # va = list(dd.values())[cnt]
                        attr_dict[ka] = v
                        cnt = cnt + 1
                update_expr = update_expr[:-2]

                print(f"PRIMARY KEY: {primary_key}:{dd.get(primary_key)}")
                # print(f"""
                #         UPDATE EXPR {update_expr}
                #         ATTR   EXPR {attr_dict}
                #         ATTR   NAME {attr_names}""")

                # check attr_names for null, if null, do not add in update_item()
                try:
                    if not attr_names:
                        a = foo_table.update_item(
                            Key={
                                primary_key: dd.get(primary_key)
                            },
                            UpdateExpression=update_expr,
                            ExpressionAttributeValues=attr_dict
                        )

                    else:
                        a = foo_table.update_item(
                            Key={
                                primary_key: dd.get(primary_key)
                            },
                            UpdateExpression=update_expr,
                            ExpressionAttributeValues=attr_dict,
                            ExpressionAttributeNames=attr_names
                        )

                except Exception as e:
                    print(f"[ERR] Error updating record due to {type(e)} MORE: {str(e)}")
                    print(f"""
                                        UPDATE EXPR {update_expr}
                                        ATTR   EXPR {attr_dict}
                                        ATTR   NAME {attr_names}""")
            return True
        except KeyError as e:
            print(f"ERR: Retrying to update item. Attempt {attempt + 1}. Error encountered {type(e)} MORE: {str(e)}")
        attempt += 1
    else:
        return False


def update_table_v2(df_dict, table_name_str, primary_key):
    """
    Update table V2 works like Upsert Query with no looping of batch in df_dict. df_dict is simple MAP or JSON structure
    for a single record. BORM 5 is applied. Returns boolean value denoting Success of Upserting the passed df_dict. This
    function works for dest table with only Partition Key and not Sort Key.
    :param dict df_dict: Single record in Dictionary format
    :param str table_name_str: table name as arrived from Env variable
    :param str primary_key: Key that needs to be searched from df_dict
    :return bool:
    """
    dyndb = boto3.resource('dynamodb')
    foo_table = dyndb.Table(table_name_str)
    attempt = 0

    DYNAMODB_RESERVED_WORDS = ['abort', 'absolute', 'action', 'add', 'after', 'agent', 'aggregate', 'all', 'allocate',
                               'alter', 'analyze', 'and', 'any', 'archive', 'are', 'array', 'as', 'asc', 'ascii',
                               'asensitive', 'assertion', 'asymmetric', 'at', 'atomic', 'attach', 'attribute', 'auth',
                               'authorization', 'authorize', 'auto', 'avg', 'back', 'backup', 'base', 'batch', 'before',
                               'begin', 'between', 'bigint', 'binary', 'bit', 'blob', 'block', 'boolean', 'both',
                               'breadth', 'bucket', 'bulk', 'by', 'byte', 'call', 'called', 'calling', 'capacity',
                               'cascade', 'cascaded', 'case', 'cast', 'catalog', 'char', 'character', 'check', 'class',
                               'clob', 'close', 'cluster', 'clustered', 'clustering', 'clusters', 'coalesce', 'collate',
                               'collation', 'collection', 'column', 'columns', 'combine', 'comment', 'commit',
                               'compact', 'compile', 'compress', 'condition', 'conflict', 'connect', 'connection',
                               'consistency', 'consistent', 'constraint', 'constraints', 'constructor', 'consumed',
                               'continue', 'convert', 'copy', 'corresponding', 'count', 'counter', 'create', 'cross',
                               'cube', 'current', 'cursor', 'cycle', 'data', 'database', 'date', 'datetime', 'day',
                               'deallocate', 'dec', 'decimal', 'declare', 'default', 'deferrable', 'deferred', 'define',
                               'defined', 'definition', 'delete', 'delimited', 'depth', 'deref', 'desc', 'describe',
                               'descriptor', 'detach', 'deterministic', 'diagnostics', 'directories', 'disable',
                               'disconnect', 'distinct', 'distribute', 'do', 'domain', 'double', 'drop', 'dump',
                               'duration', 'dynamic', 'each', 'element', 'else', 'elseif', 'empty', 'enable', 'end',
                               'equal', 'equals', 'error', 'escape', 'escaped', 'eval', 'evaluate', 'exceeded',
                               'except', 'exception', 'exceptions', 'exclusive', 'exec', 'execute', 'exists', 'exit',
                               'explain', 'explode', 'export', 'expression', 'extended', 'external', 'extract', 'fail',
                               'false', 'family', 'fetch', 'fields', 'file', 'filter', 'filtering', 'final', 'finish',
                               'first', 'fixed', 'flattern', 'float', 'for', 'force', 'foreign', 'format', 'forward',
                               'found', 'free', 'from', 'full', 'function', 'functions', 'general', 'generate', 'get',
                               'glob', 'global', 'go', 'goto', 'grant', 'greater', 'group', 'grouping', 'handler',
                               'hash', 'have', 'having', 'heap', 'hidden', 'hold', 'hour', 'identified', 'identity',
                               'if', 'ignore', 'immediate', 'import', 'in', 'including', 'inclusive', 'increment',
                               'incremental', 'index', 'indexed', 'indexes', 'indicator', 'infinite', 'initially',
                               'inline', 'inner', 'innter', 'inout', 'input', 'insensitive', 'insert', 'instead', 'int',
                               'integer', 'intersect', 'interval', 'into', 'invalidate', 'is', 'isolation', 'item',
                               'items', 'iterate', 'join', 'key', 'keys', 'lag', 'language', 'large', 'last', 'lateral',
                               'lead', 'leading', 'leave', 'left', 'length', 'less', 'level', 'like', 'limit',
                               'limited', 'lines', 'list', 'load', 'local', 'localtime', 'localtimestamp', 'location',
                               'locator', 'lock', 'locks', 'log', 'loged', 'long', 'loop', 'lower', 'map', 'match',
                               'materialized', 'max', 'maxlen', 'member', 'merge', 'method', 'metrics', 'min', 'minus',
                               'minute', 'missing', 'mod', 'mode', 'modifies', 'modify', 'module', 'month', 'multi',
                               'multiset', 'name', 'names', 'national', 'natural', 'nchar', 'nclob', 'new', 'next',
                               'no', 'none', 'not', 'null', 'nullif', 'number', 'numeric', 'object', 'of', 'offline',
                               'offset', 'old', 'on', 'online', 'only', 'opaque', 'open', 'operator', 'option', 'or',
                               'order', 'ordinality', 'other', 'others', 'out', 'outer', 'output', 'over', 'overlaps',
                               'override', 'owner', 'pad', 'parallel', 'parameter', 'parameters', 'partial',
                               'partition', 'partitioned', 'partitions', 'path', 'percent', 'percentile', 'permission',
                               'permissions', 'pipe', 'pipelined', 'plan', 'pool', 'position', 'precision', 'prepare',
                               'preserve', 'primary', 'prior', 'private', 'privileges', 'procedure', 'processed',
                               'project', 'projection', 'property', 'provisioning', 'public', 'put', 'query', 'quit',
                               'quorum', 'raise', 'random', 'range', 'rank', 'raw', 'read', 'reads', 'real', 'rebuild',
                               'record', 'recursive', 'reduce', 'ref', 'reference', 'references', 'referencing',
                               'regexp', 'region', 'reindex', 'relative', 'release', 'remainder', 'rename', 'repeat',
                               'replace', 'request', 'reset', 'resignal', 'resource', 'response', 'restore', 'restrict',
                               'result', 'return', 'returning', 'returns', 'reverse', 'revoke', 'right', 'role',
                               'roles', 'rollback', 'rollup', 'routine', 'row', 'rows', 'rule', 'rules', 'sample',
                               'satisfies', 'save', 'savepoint', 'scan', 'schema', 'scope', 'scroll', 'search',
                               'second', 'section', 'segment', 'segments', 'select', 'self', 'semi', 'sensitive',
                               'separate', 'sequence', 'serializable', 'session', 'set', 'sets', 'shard', 'share',
                               'shared', 'short', 'show', 'signal', 'similar', 'size', 'skewed', 'smallint', 'snapshot',
                               'some', 'source', 'space', 'spaces', 'sparse', 'specific', 'specifictype', 'split',
                               'sql', 'sqlcode', 'sqlerror', 'sqlexception', 'sqlstate', 'sqlwarning', 'start', 'state',
                               'static', 'status', 'storage', 'store', 'stored', 'stream', 'string', 'struct', 'style',
                               'sub', 'submultiset', 'subpartition', 'substring', 'subtype', 'sum', 'super',
                               'symmetric', 'synonym', 'system', 'table', 'tablesample', 'temp', 'temporary',
                               'terminated', 'text', 'than', 'then', 'throughput', 'time', 'timestamp', 'timezone',
                               'tinyint', 'to', 'token', 'total', 'touch', 'trailing', 'transaction', 'transform',
                               'translate', 'translation', 'treat', 'trigger', 'trim', 'true', 'truncate', 'ttl',
                               'tuple', 'type', 'under', 'undo', 'union', 'unique', 'unit', 'unknown', 'unlogged',
                               'unnest', 'unprocessed', 'unsigned', 'until', 'update', 'upper', 'url', 'usage', 'use',
                               'user', 'users', 'using', 'uuid', 'vacuum', 'value', 'valued', 'values', 'varchar',
                               'variable', 'variance', 'varint', 'varying', 'view', 'views', 'virtual', 'void', 'wait',
                               'when', 'whenever', 'where', 'while', 'window', 'with', 'within', 'without', 'work',
                               'wrapped', 'write', 'year', 'zone']

    for i in range(5):  # BackOff Retry Mechanism with 10 retries - BORM 5
        try:
            dd = df_dict

            # update_expr = expression_list[0]
            # attr_dict = expression_list[1]
            # attr_names = expression_list[2]

            dd['updated_dttm'] = (datetime.now() + relativedelta(hours=5, minutes=30)).strftime('%Y-%m-%dT%H:%M:%SZ')
            # decimal_values = {k: str(v) for k, v in dd.items() if k in ["lat", "long", "branch_code"]}
            # dd.update(decimal_values)
            dd = {k: v for k, v in dd.items() if v not in [' ', '', None]}

            cnt = 0
            update_expr = "SET "
            attr_names = dict()
            attr_dict = dict()
            for k, v in dd.items():
                if k not in [primary_key]:
                    if k.lower() in DYNAMODB_RESERVED_WORDS:
                        new_k = "#" + k[:-1]
                        attr_names.update({new_k: k})
                        k = new_k
                    update_expr = update_expr + " " + k + " = " + ":var" + str(cnt + 1) + ", "
                    ka = ':var' + str(cnt + 1)
                    # va = list(dd.values())[cnt]
                    attr_dict[ka] = v
                    cnt = cnt + 1
            update_expr = update_expr[:-2]

            print(f"PRIMARY KEY: {primary_key}: {dd.get(primary_key)}")
            # print(f"""
            #         UPDATE EXPR {update_expr}
            #         ATTR   EXPR {attr_dict}
            #         ATTR   NAME {attr_names}""")

            # check attr_names for null, if null, do not add in update_item()
            try:
                if not attr_names:
                    a = foo_table.update_item(
                        Key={
                            primary_key: dd.get(primary_key)
                        },
                        UpdateExpression=update_expr,
                        ExpressionAttributeValues=attr_dict
                    )
                    return True
                else:
                    a = foo_table.update_item(
                        Key={
                            primary_key: dd.get(primary_key)
                        },
                        UpdateExpression=update_expr,
                        ExpressionAttributeValues=attr_dict,
                        ExpressionAttributeNames=attr_names
                    )
                    return True
            except Exception as e:
                print(f"[ERR] Error updating record due to {type(e)} MORE: {str(e)}")
                print(f"""
                    UPDATE EXPR {update_expr}
                    ATTR   EXPR {attr_dict}
                    ATTR   NAME {attr_names}""")

        except KeyError as e:
            print(f"ERR: Retrying to update item. Attempt {attempt + 1}. Error encountered {type(e)} MORE: {str(e)}")
        attempt += 1
    else:
        return False


def update_table_v3(df_dict, table_name_str, primary_key, secondary_key="", skey=False, ts=True):
    """
    Update table V3 works like Upsert Query with no looping of batch in df_dict. df_dict is simple MAP or JSON structure
    for a single record. BORM 5 is applied. Returns boolean value denoting Success of Upserting the passed df_dict.
    This function works for dest table with both Partition Key and Sort Key.
    :param dict df_dict: Single record in Dictionary format
    :param str table_name_str: table name as arrived from Env variable
    :param str primary_key: Partition Key of target table from df_dict
    :param str secondary_key: Sort Key (not compulsory) of target table from df_dict
    :param bool skey: True required when passing skey
    :return bool:
    """

    dyndb = boto3.resource('dynamodb')
    foo_table = dyndb.Table(table_name_str)
    attempt = 0

    DYNAMODB_RESERVED_WORDS = ['abort', 'absolute', 'action', 'add', 'after', 'agent', 'aggregate', 'all', 'allocate',
                               'alter', 'analyze', 'and', 'any', 'archive', 'are', 'array', 'as', 'asc', 'ascii',
                               'asensitive', 'assertion', 'asymmetric', 'at', 'atomic', 'attach', 'attribute', 'auth',
                               'authorization', 'authorize', 'auto', 'avg', 'back', 'backup', 'base', 'batch', 'before',
                               'begin', 'between', 'bigint', 'binary', 'bit', 'blob', 'block', 'boolean', 'both',
                               'breadth', 'bucket', 'bulk', 'by', 'byte', 'call', 'called', 'calling', 'capacity',
                               'cascade', 'cascaded', 'case', 'cast', 'catalog', 'char', 'character', 'check', 'class',
                               'clob', 'close', 'cluster', 'clustered', 'clustering', 'clusters', 'coalesce', 'collate',
                               'collation', 'collection', 'column', 'columns', 'combine', 'comment', 'commit',
                               'compact', 'compile', 'compress', 'condition', 'conflict', 'connect', 'connection',
                               'consistency', 'consistent', 'constraint', 'constraints', 'constructor', 'consumed',
                               'continue', 'convert', 'copy', 'corresponding', 'count', 'counter', 'create', 'cross',
                               'cube', 'current', 'cursor', 'cycle', 'data', 'database', 'date', 'datetime', 'day',
                               'deallocate', 'dec', 'decimal', 'declare', 'default', 'deferrable', 'deferred', 'define',
                               'defined', 'definition', 'delete', 'delimited', 'depth', 'deref', 'desc', 'describe',
                               'descriptor', 'detach', 'deterministic', 'diagnostics', 'directories', 'disable',
                               'disconnect', 'distinct', 'distribute', 'do', 'domain', 'double', 'drop', 'dump',
                               'duration', 'dynamic', 'each', 'element', 'else', 'elseif', 'empty', 'enable', 'end',
                               'equal', 'equals', 'error', 'escape', 'escaped', 'eval', 'evaluate', 'exceeded',
                               'except', 'exception', 'exceptions', 'exclusive', 'exec', 'execute', 'exists', 'exit',
                               'explain', 'explode', 'export', 'expression', 'extended', 'external', 'extract', 'fail',
                               'false', 'family', 'fetch', 'fields', 'file', 'filter', 'filtering', 'final', 'finish',
                               'first', 'fixed', 'flattern', 'float', 'for', 'force', 'foreign', 'format', 'forward',
                               'found', 'free', 'from', 'full', 'function', 'functions', 'general', 'generate', 'get',
                               'glob', 'global', 'go', 'goto', 'grant', 'greater', 'group', 'grouping', 'handler',
                               'hash', 'have', 'having', 'heap', 'hidden', 'hold', 'hour', 'identified', 'identity',
                               'if', 'ignore', 'immediate', 'import', 'in', 'including', 'inclusive', 'increment',
                               'incremental', 'index', 'indexed', 'indexes', 'indicator', 'infinite', 'initially',
                               'inline', 'inner', 'innter', 'inout', 'input', 'insensitive', 'insert', 'instead', 'int',
                               'integer', 'intersect', 'interval', 'into', 'invalidate', 'is', 'isolation', 'item',
                               'items', 'iterate', 'join', 'key', 'keys', 'lag', 'language', 'large', 'last', 'lateral',
                               'lead', 'leading', 'leave', 'left', 'length', 'less', 'level', 'like', 'limit',
                               'limited', 'lines', 'list', 'load', 'local', 'localtime', 'localtimestamp', 'location',
                               'locator', 'lock', 'locks', 'log', 'loged', 'long', 'loop', 'lower', 'map', 'match',
                               'materialized', 'max', 'maxlen', 'member', 'merge', 'method', 'metrics', 'min', 'minus',
                               'minute', 'missing', 'mod', 'mode', 'modifies', 'modify', 'module', 'month', 'multi',
                               'multiset', 'name', 'names', 'national', 'natural', 'nchar', 'nclob', 'new', 'next',
                               'no', 'none', 'not', 'null', 'nullif', 'number', 'numeric', 'object', 'of', 'offline',
                               'offset', 'old', 'on', 'online', 'only', 'opaque', 'open', 'operator', 'option', 'or',
                               'order', 'ordinality', 'other', 'others', 'out', 'outer', 'output', 'over', 'overlaps',
                               'override', 'owner', 'pad', 'parallel', 'parameter', 'parameters', 'partial',
                               'partition', 'partitioned', 'partitions', 'path', 'percent', 'percentile', 'permission',
                               'permissions', 'pipe', 'pipelined', 'plan', 'pool', 'position', 'precision', 'prepare',
                               'preserve', 'primary', 'prior', 'private', 'privileges', 'procedure', 'processed',
                               'project', 'projection', 'property', 'provisioning', 'public', 'put', 'query', 'quit',
                               'quorum', 'raise', 'random', 'range', 'rank', 'raw', 'read', 'reads', 'real', 'rebuild',
                               'record', 'recursive', 'reduce', 'ref', 'reference', 'references', 'referencing',
                               'regexp', 'region', 'reindex', 'relative', 'release', 'remainder', 'rename', 'repeat',
                               'replace', 'request', 'reset', 'resignal', 'resource', 'response', 'restore', 'restrict',
                               'result', 'return', 'returning', 'returns', 'reverse', 'revoke', 'right', 'role',
                               'roles', 'rollback', 'rollup', 'routine', 'row', 'rows', 'rule', 'rules', 'sample',
                               'satisfies', 'save', 'savepoint', 'scan', 'schema', 'scope', 'scroll', 'search',
                               'second', 'section', 'segment', 'segments', 'select', 'self', 'semi', 'sensitive',
                               'separate', 'sequence', 'serializable', 'session', 'set', 'sets', 'shard', 'share',
                               'shared', 'short', 'show', 'signal', 'similar', 'size', 'skewed', 'smallint', 'snapshot',
                               'some', 'source', 'space', 'spaces', 'sparse', 'specific', 'specifictype', 'split',
                               'sql', 'sqlcode', 'sqlerror', 'sqlexception', 'sqlstate', 'sqlwarning', 'start', 'state',
                               'static', 'status', 'storage', 'store', 'stored', 'stream', 'string', 'struct', 'style',
                               'sub', 'submultiset', 'subpartition', 'substring', 'subtype', 'sum', 'super',
                               'symmetric', 'synonym', 'system', 'table', 'tablesample', 'temp', 'temporary',
                               'terminated', 'text', 'than', 'then', 'throughput', 'time', 'timestamp', 'timezone',
                               'tinyint', 'to', 'token', 'total', 'touch', 'trailing', 'transaction', 'transform',
                               'translate', 'translation', 'treat', 'trigger', 'trim', 'true', 'truncate', 'ttl',
                               'tuple', 'type', 'under', 'undo', 'union', 'unique', 'unit', 'unknown', 'unlogged',
                               'unnest', 'unprocessed', 'unsigned', 'until', 'update', 'upper', 'url', 'usage', 'use',
                               'user', 'users', 'using', 'uuid', 'vacuum', 'value', 'valued', 'values', 'varchar',
                               'variable', 'variance', 'varint', 'varying', 'view', 'views', 'virtual', 'void', 'wait',
                               'when', 'whenever', 'where', 'while', 'window', 'with', 'within', 'without', 'work',
                               'wrapped', 'write', 'year', 'zone']

    for i in range(5):  # BackOff Retry Mechanism with 10 retries - BORM 5
        try:
            dd = df_dict

            # update_expr = expression_list[0]
            # attr_dict = expression_list[1]
            # attr_names = expression_list[2]

            if ts:
                dd['updated_dttm'] = (datetime.now() + relativedelta(hours=5, minutes=30)).strftime('%Y-%m-%dT%H:%M:%SZ')
            # decimal_values = {k: str(v) for k, v in dd.items() if k in ["lat", "long", "branch_code"]}
            # dd.update(decimal_values)
            dd = {k: v for k, v in dd.items() if v not in [' ', '', None]}

            cnt = 0
            update_expr = "SET "
            attr_names = dict()
            attr_dict = dict()
            for k, v in dd.items():
                if k not in [primary_key, secondary_key]:
                    if k.lower() in DYNAMODB_RESERVED_WORDS:
                        new_k = "#" + k[:-1]
                        attr_names.update({new_k: k})
                        k = new_k
                    update_expr = update_expr + " " + k + " = " + ":var" + str(cnt + 1) + ", "
                    ka = ':var' + str(cnt + 1)
                    # va = list(dd.values())[cnt]
                    attr_dict[ka] = v
                    cnt = cnt + 1
            update_expr = update_expr[:-2]

            print(f"PRIMARY KEY: {primary_key}: {dd.get(primary_key)}")
            # print(f"""
            #         UPDATE EXPR {update_expr}
            #         ATTR   EXPR {attr_dict}
            #         ATTR   NAME {attr_names}""")

            # check attr_names for null, if null, do not add in update_item()

            target_keys = {
                primary_key: dd.get(primary_key)
            }

            if skey:
                target_keys[secondary_key] = dd[secondary_key]

            try:
                if not attr_names:
                    a = foo_table.update_item(
                        Key=target_keys,
                        UpdateExpression=update_expr,
                        ExpressionAttributeValues=attr_dict
                    )
                    return True
                else:
                    a = foo_table.update_item(
                        Key=target_keys,
                        UpdateExpression=update_expr,
                        ExpressionAttributeValues=attr_dict,
                        ExpressionAttributeNames=attr_names
                    )
                    return True
            except Exception as e:
                print(f"[ERR] Error updating record due to {type(e)} MORE: {str(e)}")
                print(f"""
                    UPDATE EXPR {update_expr}
                    ATTR   EXPR {attr_dict}
                    ATTR   NAME {attr_names}""")

        except KeyError as e:
            print(f"ERR: Retrying to update item. Attempt {attempt + 1}. Error encountered {type(e)} MORE: {str(e)}")
        attempt += 1
    else:
        return False

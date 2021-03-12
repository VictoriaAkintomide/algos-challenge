from datetime import date
import boto3
import sys
import sqlite3

# Connect to the database
DATABASE = ****
cluster_arn = *****
secret_arn = *****

# Connect to local DB
connection = sqlite3.connect("analysis_data.db")
cursor = connection.cursor()

def query_db(query):
    rdsData = boto3.client('rds-data', region_name='eu-central-1')

    res = rdsData.execute_statement(
        resourceArn=cluster_arn,
        secretArn=secret_arn,
        database=DATABASE,
        sql=query,
        includeResultMetadata=True)
    del rdsData
    
    return res


def query_db_get_response(query):

    res = query_db(query)

    cols = res['columnMetadata']
    recs = res['records']
    results = []
    print('len(recs)', len(recs))
    for r in recs:
        res_dict = {}
        for i, col in enumerate(r):
            col_name = cols[i]['name']
            col_type = cols[i]['typeName']
            val = list(col.values())[0]
            if col_type == 'DECIMAL':
                res_dict[col_name] = int(val)
            else:
                res_dict[col_name] = val
        results.append(res_dict)
    return results


def insert_into_db(query):
    rdsData = boto3.client('rds-data', region_name='eu-central-1')

    tr = rdsData.begin_transaction(
        resourceArn=cluster_arn,
        secretArn=secret_arn,
        database=DATABASE)

    response = rdsData.execute_statement(
        resourceArn=cluster_arn,
        secretArn=secret_arn,
        database=DATABASE,
        sql=query,
        transactionId=tr['transactionId'])

    cr = rdsData.commit_transaction(
        resourceArn=cluster_arn,
        secretArn=secret_arn,
        transactionId=tr['transactionId'])

    return response['numberOfRecordsUpdated'], cr['transactionStatus']


def restore_from_db(item_id=None, verbose=False):
    query = "select item_id, item_group_id, SUM(num_success) as num_success, SUM(num_impressions) as num_impressions, SUM(num_engagements) as num_engagements, SUM(num_clickthroughs) as num_clickthroughs, SUM(num_trials) as num_trials, SUM(daily_spend) as daily_spend, SUM(revenue) as revenue from bayesian_bandit GROUP BY item_id;"

    if item_id is not None:
        query = f"select item_id, item_group_id, SUM(num_success) as num_success, SUM(num_impressions) as num_impressions, SUM(num_engagements) as num_engagements, SUM(num_clickthroughs) as num_clickthroughs, SUM(num_trials) as num_trials, SUM(daily_spend) as daily_spend, SUM(revenue) as revenue from bayesian_bandit where item_id=`{item_id}`"

    if verbose:
        print(query, file=sys.stderr)
    return query_db_get_response(query)



def dump_db(verbose=True, item_id_list = [], item_group_id_list = []):
    
    add_query = ''
    
    # if item_id is not None:
    #     add_query = f" where item_id='{item_id}'"


    # if item_group_id is not None:
    #     if item_id is not None:
    #         add_query += f" and item_group_id='{item_group_id}'"
    #     else:
    #         add_query = f" where item_group_id='{item_group_id}'"

    #check whether '' is used or ``

    joiner_str = ' where '
    for itm in item_id_list:
        add_query += f" {joiner_str} item_id='{itm}'"
        joiner_str = ' or '

    for itmg in item_group_id_list:
        add_query += f" {joiner_str} item_group_id='{itmg}'"
        joiner_str = ' or '

    query = f"select * from bayesian_bandit {add_query} ORDER BY item_id;"


    if verbose:
        print(query, file=sys.stderr)
        # print("SADSA", file=sys.stderr)
    
    return query_db_get_response(query)



def update_insert_database(item_id=None, num_engagements=0, num_impressions=0,num_clickthroughs=0, daily_spend=0, num_success=0, num_failures=0, num_trials=0, date=None, item_group_id=None, revenue=0, verbose=False):

    assert item_id is not None, f'item_id cannot be None'
    assert date is not None, f'date cannot be None'
    assert item_group_id is not None, f'item_group_id cannot be None'

    # check if row already exists
    query = f"select id, item_id, date from bayesian_bandit where item_id='{item_id}' and date='{date}';"

    if verbose:
        print(query, file=sys.stderr)
    res = query_db_get_response(query)

    if verbose:
        print('res', res, len(res), file=sys.stderr)

    if len(res) > 0:
        for r_ in res:
            query_db(f"delete from bayesian_bandit where id={r_['id']};")

    # if not, great a new
    cols = "item_id,  num_success,  num_trials, num_impressions, num_engagements, num_clickthroughs, revenue, date, daily_spend, item_group_id"
    vals = f"'{item_id}',{num_success}, {num_trials}, {num_impressions}, {num_engagements}, {num_clickthroughs}, {revenue}, '{date}', {daily_spend},'{item_group_id}'"

    query = f"INSERT INTO bayesian_bandit ({cols}) VALUES ({vals})"
    if verbose:
        print(query, file=sys.stderr)
    rr = insert_into_db(query)

    return rr

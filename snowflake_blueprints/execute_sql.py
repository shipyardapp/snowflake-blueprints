import argparse
import snowflake.connector


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--username', dest='username', required=True)
    parser.add_argument('--password', dest='password', required=False)
    parser.add_argument('--account', dest='account', required=True)
    parser.add_argument('--database', dest='database', required=True)
    parser.add_argument('--schema', dest='schema', required=False)
    parser.add_argument('--query', dest='query', required=True)
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    username = args.username
    password = args.password
    account = args.account
    database = args.database
    schema = args.schema
    query = args.query

    try:
        con = snowflake.connector.connect(user=username, password=password,
                                          account=account, database=database,
                                          schema=schema)
        cur = con.cursor()
    except Exception as e:
        print(f'Failed to connect to Snowflake with user {username}')

    cur.execute(query)
    print('Your query has been successfully executed.')


if __name__ == '__main__':
    main()


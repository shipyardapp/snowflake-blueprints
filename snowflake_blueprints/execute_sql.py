import argparse
import snowflake.connector
from snowflake.connector.errors import DatabaseError, ForbiddenError, ProgrammingError
import sys
import code

EXIT_CODE_UNKNOWN_ERROR = 3
EXIT_CODE_INVALID_CREDENTIALS = 200
EXIT_CODE_INVALID_ACCOUNT = 201
EXIT_CODE_INVALID_WAREHOUSE = 202
EXIT_CODE_INVALID_DATABASE = 203
EXIT_CODE_INVALID_SCHEMA = 204
EXIT_CODE_INVALID_QUERY = 205


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--username', dest='username', required=True)
    parser.add_argument('--password', dest='password', required=False)
    parser.add_argument('--account', dest='account', required=True)
    parser.add_argument('--warehouse', dest='warehouse', required=False)
    parser.add_argument('--database', dest='database', required=True)
    parser.add_argument('--schema', dest='schema', required=False)
    parser.add_argument('--query', dest='query', required=True)
    args = parser.parse_args()
    return args


def validate_database(con, database):
    result = con.cursor().execute(
        f"SHOW DATABASES LIKE '{database}'").fetchone()
    if not result:
        print('Database provided does not exist. Please check for typos and try again.')
        sys.exit(EXIT_CODE_INVALID_DATABASE)
    return


def main():
    args = get_args()
    username = args.username
    password = args.password
    account = args.account
    warehouse = args.warehouse
    database = args.database
    schema = args.schema
    query = args.query

    try:
        con = snowflake.connector.connect(user=username, password=password,
                                          account=account, warehouse=warehouse,
                                          database=database, schema=schema)
        cur = con.cursor()
        # code.interact(local=locals())
    except DatabaseError as db_e:
        if db_e.errno == 250001:
            print(f'Invalid username or password. Please check for typos and try again.')
        print(db_e)
        sys.exit(EXIT_CODE_INVALID_CREDENTIALS)
    except ForbiddenError as f_e:
        if f_e.errno == 250001:
            if '.' not in account:
                print(
                    f'Invalid account name. Instead of {account}, it might need to be something like {account}.us-east-2.aws, including the region.')
            else:
                print(
                    f'Invalid account name. Instead of {account}, it might need to be something like {account.split(".")[0]}, without the region.')
        print(f_e)
        sys.exit(EXIT_CODE_INVALID_ACCOUNT)
    except Exception as e:
        print(f'Failed to connect to Snowflake.')
        print(e)
        sys.exit(EXIT_CODE_UNKNOWN_ERROR)

    validate_database(con=con, database=database)

    try:
        cur.execute(query)
        print('Your query has been successfully executed.')
    except ProgrammingError as p_e:
        if 'SQL compilation error' in str(p_e):
            print('Your SQL contains an error. Check for typos and try again.')
            print(p_e)
            sys.exit(EXIT_CODE_INVALID_QUERY)
        print(f'Failed to execute query.')
        print(p_e)
        sys.exit(EXIT_CODE_UNKNOWN_ERROR)
    except Exception as e:
        print(f'Failed to execute query.')
        print(e)
        sys.exit(EXIT_CODE_UNKNOWN_ERROR)


if __name__ == '__main__':
    main()

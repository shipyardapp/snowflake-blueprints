import snowflake.connector
from snowflake.connector.errors import DatabaseError, ForbiddenError
import argparse
import os
import pandas as pd
from pandas.io.sql import DatabaseError
import sys
import shipyard_utils as shipyard

try:
    import errors
except BaseException:
    from . import errors


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--username', dest='username', required=True)
    parser.add_argument('--password', dest='password', required=False)
    parser.add_argument('--account', dest='account', required=True)
    parser.add_argument('--warehouse', dest='warehouse', required=False)
    parser.add_argument('--database', dest='database', required=True)
    parser.add_argument('--schema', dest='schema', required=False)
    parser.add_argument('--query', dest='query', required=True)
    parser.add_argument(
        '--destination-file-name',
        dest='destination_file_name',
        default='output.csv',
        required=True)
    parser.add_argument(
        '--destination-folder-name',
        dest='destination_folder_name',
        default='',
        required=False)
    parser.add_argument('--file-header', dest='file_header', default='True',
                        required=False)
    args = parser.parse_args()
    return args


def create_csv(query, db_connection, destination_file_path, file_header=True):
    """
    Read in data from a SQL query. Store the data as a csv.
    """
    i = 1
    try:
        for chunk in pd.read_sql_query(query, db_connection, chunksize=10000):
            if i == 1:
                if len(chunk) == 0:
                    print('Query did not return with any data, so no file was created.')
                    sys.exit(errors.EXIT_CODE_NO_RESULTS)
                else:
                    chunk.to_csv(destination_file_path, mode='a',
                                 header=file_header, index=False)
            else:
                chunk.to_csv(destination_file_path, mode='a',
                             header=False, index=False)
            i += 1
        if not os.path.exists(destination_file_path):
            print('Query did not return with any data, so no file was created.')
            sys.exit(errors.EXIT_CODE_NO_RESULTS)
        else:
            print(
                f'Successfully stored query results as {destination_file_path}')
    except DatabaseError as db_e:
        if 'No active warehouse' in db_e.args[0]:
            print(
                f'The warehouse provided either does not exist or your user does not have access to it. If no warehouse was provided, this user does not have a default warehouse.')
            print(db_e)
            sys.exit(errors.EXIT_CODE_INVALID_WAREHOUSE)
        if 'SQL compilation error' in str(db_e):
            print('Your SQL contains an error. Check for typos and try again.')
            print(db_e)
            sys.exit(errors.EXIT_CODE_INVALID_QUERY)
        print('Failed to create a file with data.')
        print(db_e)
        sys.exit(errors.EXIT_CODE_UNKNOWN_ERROR)
    except Exception as e:
        print('Failed to create a file with data.')
        print(e)
        sys.exit(errors.EXIT_CODE_UNKNOWN_ERROR)
    return


def validate_database(con, database):
    """
    Check against the list of databases the user has access to to verify if the provided database matches.
    """
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
    destination_file_name = args.destination_file_name
    destination_folder_name = shipyard.files.clean_folder_name(
        args.destination_folder_name)
    destination_full_path = shipyard.files.combine_folder_and_file_name(
        folder_name=destination_folder_name, file_name=destination_file_name)
    file_header = shipyard.args.convert_to_boolean(args.file_header)

    try:
        con = snowflake.connector.connect(user=username, password=password,
                                          account=account, warehouse=warehouse,
                                          database=database, schema=schema)

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
        if e.errno == 250001:
            print(f'Invalid username or password. Please check for typos and try again.')
            print(e)
            sys.exit(EXIT_CODE_INVALID_CREDENTIALS)
        print(f'Failed to connect to Snowflake.')
        print(e)
        sys.exit(EXIT_CODE_UNKNOWN_ERROR)

    if not os.path.exists(destination_folder_name) and (
            destination_folder_name != ''):
        os.makedirs(destination_folder_name)

    validate_database(con=con, database=database)

    create_csv(
        query=query,
        db_connection=con,
        destination_file_path=destination_full_path,
        file_header=file_header)


if __name__ == '__main__':
    main()

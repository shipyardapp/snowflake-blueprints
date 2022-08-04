from sqlalchemy.exc import DatabaseError, DBAPIError, ProgrammingError
import pandas as pd
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import argparse
import os
import glob
import re
import sys
import shipyard_utils as shipyard
import snowflake.connector
from snowflake.connector.pandas_tools import pd_writer, write_pandas
import time

import warnings
warnings.simplefilter(action='ignore', category=UserWarning)
warnings.filterwarnings(
    action='ignore',
    message='Dialect snowflake:snowflake will not make use of SQL compilation caching.*')


EXIT_CODE_UNKNOWN_ERROR = 3
EXIT_CODE_INVALID_CREDENTIALS = 200
EXIT_CODE_INVALID_ACCOUNT = 201
EXIT_CODE_INVALID_WAREHOUSE = 202
EXIT_CODE_INVALID_DATABASE = 203
EXIT_CODE_INVALID_SCHEMA = 204
EXIT_CODE_FILE_NOT_FOUND = 205
EXIT_CODE_INVALID_UPLOAD_VALUE = 206
EXIT_CODE_INVALID_UPLOAD_COLUMNS = 207


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--username', dest='username', required=True)
    parser.add_argument('--password', dest='password', required=True)
    parser.add_argument('--account', dest='account', required=True)
    parser.add_argument('--warehouse', dest='warehouse', required=False)
    parser.add_argument('--database', dest='database', required=False)
    parser.add_argument('--schema',
                        dest='schema',
                        default=None,
                        required=False)
    parser.add_argument('--source-file-name-match-type',
                        dest='source_file_name_match_type',
                        default='exact_match',
                        choices={
                            'exact_match',
                            'regex_match'},
                        required=False)
    parser.add_argument('--source-file-name', dest='source_file_name',
                        default='output.csv', required=True)
    parser.add_argument('--source-folder-name',
                        dest='source_folder_name', default='', required=False)
    parser.add_argument('--table-name', dest='table_name', default=None,
                        required=True)
    parser.add_argument(
        '--insert-method',
        dest='insert_method',
        choices={
            'fail',
            'replace',
            'append'},
        default='append',
        required=False)
    parser.add_argument(
        '--upload-method',
        dest='upload_method',
        choices={
            'insert_statements',
            'copy_into'},
        default='copy_into',
        required=False)
    args = parser.parse_args()

    return args


def upload_data_with_put(source_full_path,
                         table_name,
                         insert_method,
                         db_connection):

    try:
        chunksize = 10000
        for index, chunk in enumerate(
                pd.read_csv(source_full_path, chunksize=chunksize)):
            chunk.head(0).to_sql(
                table_name,
                con=db_connection,
                if_exists="fail",
                index=False)
    except BaseException as e:
        print(e)
    if insert_method == 'replace':
        db_connection.execute(f"TRUNCATE {table_name}")
        db_connection.execute(f"PUT file://{source_full_path} @%{table_name}")
        db_connection.execute(
            f"copy into {table_name} FILE_FORMAT=(type=csv,SKIP_HEADER=1) PURGE=TRUE")
    elif insert_method == 'append':
        db_connection.execute(f"PUT file://{source_full_path} @%{table_name}")
        db_connection.execute(
            f"copy into {table_name} FILE_FORMAT=(type=csv,SKIP_HEADER=1) PURGE=TRUE")


def upload_data_with_write_pandas(
        source_full_path,
        table_name,
        insert_method,
        con):
    try:
        for index, chunk in enumerate(
                pd.read_csv(source_full_path, chunksize=10000)):

            if insert_method == 'replace' and index > 0:
                # First chunk replaces the table, the following chunks
                # append to the end.
                insert_method = 'append'

            chunk.columns = map(lambda x: str(x).upper(), chunk.columns)
            write_pandas(conn=con, df=chunk, table_name=table_name)
    except BaseException as e:
        print(e)


def upload_data(
        source_full_path,
        table_name,
        insert_method,
        db_connection,
        upload_method):
    try:
        chunksize = 10000
        for index, chunk in enumerate(
                pd.read_csv(source_full_path, chunksize=chunksize)):

            if insert_method == 'replace' and index > 0:
                # First chunk replaces the table, the following chunks
                # append to the end.
                insert_method = 'append'

            chunk.columns = map(lambda x: str(x).upper(), chunk.columns)
            print(
                f'Uploading rows {(chunksize*index)+1}-{chunksize * (index+1)}')
            if upload_method == 'insert_statements':
                chunk.to_sql(
                    table_name,
                    con=db_connection,
                    index=False,
                    if_exists=insert_method,
                    method='multi',
                    chunksize=10000)
            elif upload_method == 'copy_into':
                chunk.to_sql(
                    table_name,
                    con=db_connection,
                    index=False,
                    if_exists=insert_method,
                    method=pd_writer,
                    chunksize=10000)
    except DatabaseError as db_e:
        if 'No active warehouse' in str(db_e):
            print(
                f'The warehouse provided either does not exist or your user does not have access to it. If no warehouse was provided, this user does not have a default warehouse.')
            print(db_e.orig)  # Avoids printing data to console
            try:
                # Addresses issue where sometimes an empty table gets created,
                # preventing future uses.
                db_connection.execute(f'DROP TABLE {table_name}')
            except BaseException:
                pass
            sys.exit(EXIT_CODE_INVALID_WAREHOUSE)
        if 'session does not have a current database' in str(db_e):
            print(
                f'The database provided either does not exist or your user does not have access to it. If no database was provided, this user does not have a default warehouse.')
            print(db_e.orig)  # Avoids printing data to console
            sys.exit(EXIT_CODE_INVALID_DATABASE)
        if 'is not recognized' in str(db_e):
            print(
                'One or more of the values in your file does not match data_type of its column.')
            print(db_e.orig)  # Avoids printing data to console
            sys.exit(EXIT_CODE_INVALID_UPLOAD_VALUE)
        if 'This session does not have a current schema.' in str(db_e):
            print(f'The schema provided either does not exist or your user does not have access to it. If no schema was provided, no default schema exists.')
            print(db_e)
            sys.exit(EXIT_CODE_INVALID_SCHEMA)
        print(f'Failed to upload file to Snowflake.')
        print(db_e.orig)  # Avoids printing data to console
        sys.exit(EXIT_CODE_UNKNOWN_ERROR)
    except Exception as e:
        if 'No such file or directory:' in str(e):
            print(
                f'The combination of folder name/file name that you provided ({source_full_path}) could not be found. Please check for typos and try again.')
            print(e)
            sys.exit(EXIT_CODE_FILE_NOT_FOUND)
        if 'C error: Expected' and 'fields' and 'saw' in str(e):
            print('File contents don\'t match the provided headers. One or more rows contain more columns than expected.')
            print(e)
            sys.exit(EXIT_CODE_INVALID_UPLOAD_COLUMNS)
        print(f'Failed to upload file to Snowflake.')
        print(e)
        sys.exit(EXIT_CODE_UNKNOWN_ERROR)
    print(f'{source_full_path} successfully uploaded to {table_name}.')


def main():
    start = time.perf_counter()
    print(start)
    args = get_args()
    source_file_name_match_type = args.source_file_name_match_type
    source_file_name = args.source_file_name
    source_folder_name = args.source_folder_name
    source_full_path = shipyard.files.combine_folder_and_file_name(
        folder_name=source_folder_name, file_name=source_file_name)
    table_name = args.table_name
    insert_method = args.insert_method
    upload_method = args.upload_method

    try:
        db_connection = create_engine(URL(
            account=args.account,
            user=args.username,
            password=args.password,
            database=args.database,
            schema=args.schema,
            warehouse=args.warehouse
        ))
        db_connection.connect()
    except DatabaseError as db_e:
        if 'Incorrect username or password' in str(db_e):
            print(f'Invalid username or password. Please check for typos and try again.')
            print(db_e)
            sys.exit(EXIT_CODE_INVALID_CREDENTIALS)
        print(db_e)
        sys.exit(EXIT_CODE_UNKNOWN_ERROR)
    except DBAPIError as dbapi_e:
        if 'Verify the account name is correct' in str(dbapi_e):
            if '.' not in args.account:
                print(
                    f'Invalid account name. Instead of {args.account}, it might need to be something like {args.account}.us-east-2.aws, including the region.')
            else:
                print(
                    f'Invalid account name. Instead of {args.account}, it might need to be something like {args.account.split(".")[0]}, without the region.')
            print(dbapi_e)
            sys.exit(EXIT_CODE_INVALID_ACCOUNT)
        print(dbapi_e)
        sys.exit(EXIT_CODE_UNKNOWN_ERROR)
    except Exception as e:
        if 'quote_from_bytes() expected bytes' in str(e):
            print(f'The schema provided either does not exist or your user does not have access to it. If no schema was provided, no default schema exists.')
            sys.exit(EXIT_CODE_INVALID_SCHEMA)
        print(f'Failed to connect to Snowflake.')
        print(e)
        sys.exit(EXIT_CODE_UNKNOWN_ERROR)

    if source_file_name_match_type == 'regex_match':
        file_names = shipyard.files.find_all_local_file_names(
            source_folder_name)
        matching_file_names = shipyard.files.find_all_file_matches(
            file_names, re.compile(source_file_name))
        print(f'{len(matching_file_names)} files found. Preparing to upload...')

        for index, key_name in enumerate(matching_file_names):
            upload_data(
                source_full_path=key_name,
                table_name=table_name,
                insert_method=insert_method,
                db_connection=db_connection,
                upload_method=upload_method)

    else:
        # upload_data(
        #     source_full_path=source_full_path,
        #     table_name=table_name,
        #     insert_method=insert_method,
        #     db_connection=db_connection,
        #     upload_method=upload_method)
        upload_data_with_put(
            source_full_path,
            table_name,
            insert_method,
            db_connection)

    db_connection.dispose()
    finish = time.perf_counter()
    print(finish)
    print(f'It took {finish - start} seconds to upload 10mb of data, using the put upload method and a chunksize of 10k.')


if __name__ == '__main__':
    main()

from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import argparse
import os
import glob
import re
import pandas as pd
import sys
import code

from sqlalchemy.exc import DatabaseError, DBAPIError, ProgrammingError

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
    parser.add_argument(
        '--schema',
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
    args = parser.parse_args()

    return args


def find_all_local_file_names(source_folder_name):
    """
    Returns a list of all files that exist in the current working directory,
    filtered by source_folder_name if provided.
    """
    cwd = os.getcwd()
    cwd_extension = os.path.normpath(f'{cwd}/{source_folder_name}/**')
    file_names = glob.glob(cwd_extension, recursive=True)
    return [file_name for file_name in file_names if os.path.isfile(file_name)]


def find_all_file_matches(file_names, file_name_re):
    """
    Return a list of all file_names that matched the regular expression.
    """
    matching_file_names = []
    for file in file_names:
        if re.search(file_name_re, file):
            matching_file_names.append(file)

    return matching_file_names


def combine_folder_and_file_name(folder_name, file_name):
    """
    Combine together the provided folder_name and file_name into one path variable.
    """
    combined_name = os.path.normpath(
        f'{folder_name}{"/" if folder_name else ""}{file_name}')

    return combined_name


def upload_data(source_full_path, table_name, insert_method, db_connection):
    try:
        for chunk in pd.read_csv(source_full_path, chunksize=10000):
            chunk.to_sql(
                table_name,
                con=db_connection,
                index=False,
                if_exists=insert_method,
                method='multi',
                chunksize=1000)
        print(f'{source_full_path} successfully uploaded to {table_name}.')
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


def main():
    args = get_args()
    source_file_name_match_type = args.source_file_name_match_type
    source_file_name = args.source_file_name
    source_folder_name = args.source_folder_name
    source_full_path = combine_folder_and_file_name(
        folder_name=source_folder_name, file_name=source_file_name)
    table_name = args.table_name
    insert_method = args.insert_method

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
        file_names = find_all_local_file_names(source_folder_name)
        matching_file_names = find_all_file_matches(
            file_names, re.compile(source_file_name))
        print(f'{len(matching_file_names)} files found. Preparing to upload...')

        for index, key_name in enumerate(matching_file_names):
            upload_data(
                source_full_path=key_name,
                table_name=table_name,
                insert_method=insert_method,
                db_connection=db_connection)

    else:
        upload_data(source_full_path=source_full_path, table_name=table_name,
                    insert_method=insert_method, db_connection=db_connection)

    db_connection.dispose()


if __name__ == '__main__':
    main()

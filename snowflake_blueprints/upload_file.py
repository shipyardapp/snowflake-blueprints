from sqlalchemy.exc import DatabaseError, DBAPIError, ProgrammingError
import pandas as pd
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import argparse
import re
import sys
import shipyard_utils as shipyard
from snowflake.connector.pandas_tools import pd_writer, write_pandas
import dask.dataframe as dd
import snowflake.connector
import ast
import snowflake.sqlalchemy as sql
from sqlalchemy import Table, Column, Integer, String, MetaData
from copy import deepcopy
try:
    import errors
    import utils
except BaseException:
    from . import errors, utils
import warnings
import numpy as np
warnings.simplefilter(action='ignore', category=UserWarning)
warnings.filterwarnings(
    action='ignore',
    message='Dialect snowflake:snowflake will not make use of SQL compilation caching.*')


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--username', dest='username', required=True)
    parser.add_argument('--password', dest='password', required=False)
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
            'append'
        },
        default='append',
        required=False)
    parser.add_argument(
        "--snowflake-data-types", dest="snowflake_data_types", required=False, default=''
    )
    parser.add_argument('--user-role', dest='user_role', required=False, default = '')
    parser.add_argument('--private-key-path', dest='private_key_path', required=False, default = '')
    parser.add_argument('--private-key-passphrase', dest='private_key_passphrase', required=False, default = '')
    args = parser.parse_args()

    return args


def map_snowflake_to_pandas(snowflake_data_types):
    """ Helper function to map a snowflake data type to the associated pandas data type

    Args:
        List 

    Returns:
        dict | None: Dict where the key is the field name and the value is the pandas data type
    """
    if snowflake_data_types is None:
        return None
    snowflake_to_pandas = {
        'BOOLEAN': 'bool',
        'TINYINT': 'int8',
        'SMALLINT': 'int16',
        'INTEGER': 'int32',
        'INT': 'int32',
        'BIGINT': 'int64',
        'FLOAT': 'float32',
        'DOUBLE': 'float64',
        'DECIMAL': 'float64',
        'NUMERIC': 'float64',
        'NUMBER': 'float64',
        'REAL': 'float32',
        'DATE': 'datetime64[ns]',
        'TIME': 'datetime64[ns]',
        'DATETIME' : 'datetime64[ns]',
        'TIMESTAMP': 'datetime64[ns]',
        'VARCHAR': 'object',
        'NVARCHAR': 'object',
        'CHAR': 'object',
        'NCHAR': 'object',
        'BINARY': 'object',
        'VARBINARY': 'object',
        'STRING': 'object'}

    pandas_dtypes = {}
    for item in snowflake_data_types:
        field = item[0]
        dtype = item[1]
        try:
            converted = snowflake_to_pandas[str(dtype).upper()]
            if converted is None:
                print(
                    f"The datatype {field} is not a recognized snowflake datatype")
                sys.exit(errors.EXIT_CODE_INVALID_DATA_TYPES)
            pandas_dtypes[field] = converted
        except KeyError as e:
            print(
                f"The datatype {field} is not a recognized snowflake datatype")
            sys.exit(errors.EXIT_CODE_INVALID_DATA_TYPES)

    return pandas_dtypes


def get_pandas_dates(pandas_datatypes: dict) -> tuple:
    dates = []
    new_dict = deepcopy(pandas_datatypes)
    for k, v in pandas_datatypes.items():
        if v in ['datetime64[ns]', 'datetime64']:
            dates.append(k)
            del new_dict[k]
    if len(dates) == 0:
        return None, new_dict
    return dates, new_dict


def create_table_with_types(table_name, db_connection, data_types):
    """
    Creates a table with specific data types or replaces a table if it already exists.
    Replacement will wipe the data in the existing table and then set the columns with the appropriate data types
    """
    try:
        temp_name = f"temp_{table_name}"
        query = f"create or replace temporary table {temp_name}" + "(\n"
        length = len(data_types)
        index = 1
        for datatype in data_types:
            col_name = datatype[0]
            d_type = datatype[1]
            if index < length:
                query = query + " " + col_name + " " + d_type + ", "
            else:
                query = query + " " + col_name + " " + d_type
            index += 1
        else:
            query = query + "\n );"
        db_connection.execute(query)
        print(f"Successfully created {temp_name}")
        return temp_name

    except Exception as e:
        print(f"Error in creating {table_name}")
        print(f"The query {query} contains errors")
        sys.exit(errors.EXIT_CODE_INVALID_QUERY)


def create_table(source_full_path, table_name, insert_method, db_connection, snowflake_data_types=None):
    """
    Creates a table by looking at the schema of the first 10k rows and only loading the header row.
    Used by the new PUT method because you can't PUT or COPY INTO if the table doesn't exist beforehand.
    """
    datatypes = map_snowflake_to_pandas(snowflake_data_types)
    try:
        chunksize = 10000
        mapping = map_snowflake_to_pandas(snowflake_data_types)
        dates = None
        pandas_datatypes = None
        if snowflake_data_types is not None:
            dates, pandas_datatypes = get_pandas_dates(datatypes)
        for index, chunk in enumerate(
                pd.read_csv(source_full_path, chunksize=chunksize, dtype=pandas_datatypes, parse_dates=dates)):
            chunk.head(0).to_sql(
                table_name,
                con=db_connection,
                if_exists="fail",
                index=False)
            # prevents a loop that was necessary for loading only a small chunk
            # of the data in.
            break
        if insert_method == 'append':
            print(f'Created table {table_name} because it did not exist.')
        elif insert_method == 'replace':
            print(f'Created a new table {table_name}.')
    except BaseException as e:
        if 'already exists' in str(e):
            pass
        else:
            print(e)


def convert_to_parquet(source_full_path, table_name, snowflake_datatypes):
    """
    Converts a given CSV to multiple Parquet for uploading.
    Uses fastest tested method with Dask and gzip (Snowflake recommendation)
    Parquet files allows for column name mapping.
    """
    parquet_path = f'./tmp/{table_name}'
    shipyard.files.create_folder_if_dne(parquet_path)
    datatypes = map_snowflake_to_pandas(snowflake_datatypes)
    dates = None
    pandas_datatypes = None
    if datatypes is not None:
        dates, pandas_datatypes = get_pandas_dates(datatypes)
    df = dd.read_csv(source_full_path, dtype=pandas_datatypes,
                     parse_dates=dates, date_parser=lambda x: pd.to_datetime(x).to_datetime64())
    df.columns = map(lambda x: str(x).upper(), df.columns)
    df.to_parquet(
        parquet_path,
        compression='gzip',
        write_index=False,
        write_metadata_file=False)
    return parquet_path


def compress_csv(source_full_path, table_name, snowflake_datatypes):
    """
    Compresses a CSV using GZIP to be loaded to Snowflake
    """
    csv_path = f'./tmp/{table_name}'
    full_path = f"{csv_path}/data_*.csv.gz"
    shipyard.files.create_folder_if_dne(csv_path)
    datatypes = map_snowflake_to_pandas(snowflake_datatypes)
    dates = None
    pandas_datatypes = None
    if datatypes:
        dates, pandas_datatypes = get_pandas_dates(datatypes)
    # handle cases where dates is None
    if dates:
        dates, pandas_datatypes = get_pandas_dates(datatypes)

        df = dd.read_csv(source_full_path, dtype=pandas_datatypes, parse_dates=dates,
            date_parser=lambda x: pd.to_datetime(x, errors = 'coerce',).to_datetime64())
    else:
        df = dd.read_csv(source_full_path, dtype=pandas_datatypes)

    df.columns = map(lambda x: str(x).upper(), df.columns)
    df.to_csv(full_path, compression='gzip', index=False)
    return full_path


def put_csv(db_connection, file_path, table_name, results_dict):
    """
    Execute the PUT command against Snowflake and store the results.
    """
    put_statement = f"PUT file://{file_path} '@%\"{table_name}\"'"
    put = db_connection.execute(put_statement)
    for item in put:
        # These are guesses. The documentation doesn't specify.
        put_results = {
            "input_file_name": item[0],
            "uploaded_file_name": item[1],
            "input_bytes": item[2],
            "uploaded_bytes": item[3],
            "input_file_type": item[4],
            "uploaded_file_type": item[5],
            "status": item[6]}
        results_dict['put'].append(put_results)

    return results_dict


def execute_put_command(db_connection, file_path, table_name, results_dict):
    """
    Execute the PUT command against Snowflake and store the results.
    """
    put_statement = f"PUT file://{file_path}/part.*.parquet '@%\"{table_name}\"'"
    put = db_connection.execute(put_statement)
    for item in put:
        # These are guesses. The documentation doesn't specify.
        put_results = {
            "input_file_name": item[0],
            "uploaded_file_name": item[1],
            "input_bytes": item[2],
            "uploaded_bytes": item[3],
            "input_file_type": item[4],
            "uploaded_file_type": item[5],
            "status": item[6]}
        results_dict['put'].append(put_results)

    return results_dict


def execute_drop_command(db_connection, table_name, results_dict):
    """
    Execute the DROP command against Snowflake and store the results.
    """
    drop = db_connection.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    for item in drop:
        drop_results = {"message": item[0]}
        results_dict['drop'].append(drop_results)
    return results_dict


def create_file_format(db_connection):
    file_format = "t_csv"
    sql = f""" CREATE OR REPLACE FILE FORMAT {file_format}
    TYPE = "CSV"
    COMPRESSION = "GZIP"
    FILE_EXTENSION= 'csv.gz'
    SKIP_HEADER = 1
    EMPTY_FIELD_AS_NULL = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;
    """
    db_connection.execute(sql)
    return file_format


def execute_copyinto_command(db_connection, table_name, results_dict):
    """
    Execute the COPY INTO command against Snowflake and store the results.
    """
    # copy_into_statement = f'COPY INTO "{table_name}" FILE_FORMAT=(type=PARQUET) PURGE=TRUE MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE'
    format_name = create_file_format(db_connection)
    copy_into_statement = f"""COPY INTO "{table_name}" FILE_FORMAT='{format_name}' PURGE=TRUE"""
    copy = db_connection.execute(copy_into_statement)
    for item in copy:
        copy_results = {
            "file": item[0],
            "status": item[1],
            "rows_parsed": item[2],
            "rows_loaded": item[3],
            "error_limit": item[4],
            "errors_seen": item[5],
            "first_error": item[6],
            "first_error_line": item[7],
            "first_error_character": item[8],
            "first_error_column_name": item[9]}
        results_dict['copy'].append(copy_results)
    return results_dict


def upload_data_with_put(source_full_path,
                         table_name,
                         insert_method,
                         db_connection,
                         snowflake_data_types=None):
    """
    Upload data by PUTing the file(s) in Snowflake temporary storage and using COPY INTO to get them into the table.
    """
    csv_path = compress_csv(source_full_path, table_name, snowflake_data_types)
    print('Attempting upload with put method')
    snowflake_results = {"put": [], "copy": [], "drop": []}
    if insert_method == 'replace':
        snowflake_results = execute_drop_command(
            db_connection, table_name, snowflake_results)
        create_table(
            source_full_path,
            table_name,
            insert_method,
            db_connection,
            snowflake_data_types)
        snowflake_results = put_csv(
            db_connection, csv_path, table_name, snowflake_results)
        snowflake_results = execute_copyinto_command(
            db_connection, table_name, snowflake_results)
    elif insert_method == 'append':
        create_table(
            source_full_path,
            table_name,
            insert_method,
            db_connection,
            snowflake_data_types)
        snowflake_results = put_csv(
            db_connection, csv_path, table_name, snowflake_results)
        snowflake_results = execute_copyinto_command(
            db_connection, table_name, snowflake_results)
    print(f'{source_full_path} successfully {insert_method}{"ed to " if insert_method == "append" else "d "}the table {table_name}.')
    return snowflake_results


def upload_data_with_insert(source_full_path,
                            table_name,
                            insert_method,
                            db_connection,
                            snowflake_data_types=None):
    """
    Upload the data using pandas.to_sql which creates multiple INSERT statements.
    """
    print('Attempting upload with insert method')
    chunksize = 10000
    datatypes = map_snowflake_to_pandas(snowflake_data_types)
    dates = None
    pandas_datatypes = None
    if datatypes is not None:
        dates, pandas_datatypes = get_pandas_dates(datatypes)
    for index, chunk in enumerate(
            pd.read_csv(source_full_path, chunksize=chunksize, dtype=pandas_datatypes, parse_dates=dates)):

        if insert_method == 'replace' and index > 0:
            # First chunk replaces the table, the following chunks
            # append to the end.
            insert_method = 'append'

        chunk.columns = map(lambda x: str(x).upper(), chunk.columns)
        print(f'Uploading rows {(chunksize*index)+1}-{chunksize * (index+1)}')
        chunk.to_sql(
            table_name,
            con=db_connection,
            index=False,
            if_exists=insert_method,
            method='multi',  # Not using pd_writer method due to inability to create tables and consistency with old behavior
            chunksize=10000)


def upload_data(
        source_full_path,
        table_name,
        insert_method,
        db_connection,
        snowflake_data_types=None):
    """
    Upload the data to Snowflake. Tries the PUT method first, then relies on INSERT as a backup.
    """
    # Try to use put method initially.
    try:
        snowflake_results = upload_data_with_put(source_full_path,
                                                 table_name,
                                                 insert_method,
                                                 db_connection,
                                                 snowflake_data_types)

        # create artifacts folder to save responses
        # TODO Both errors and successes need to be returned and printed out afterwards.
        # Didn't build this in initially for simplicity of getting the backup old method working.
        base_folder_name = shipyard.logs.determine_base_artifact_folder(
            'snowflake')
        artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(
            base_folder_name)
        shipyard.logs.create_artifacts_folders(artifact_subfolder_paths)
        snowflake_upload_response_path = shipyard.files.combine_folder_and_file_name(
            artifact_subfolder_paths['responses'], f'upload_{table_name}_response.json')
        shipyard.files.write_json_to_file(
            snowflake_results,
            snowflake_upload_response_path)

        # Needed to prevent other try from running if this is successful.
        return snowflake_results
    except ProgrammingError as pg_e:
        if 'This session does not have a current schema.' in str(pg_e):
            print(f'The schema provided either does not exist or your user does not have access to it. If no schema was provided, no default schema exists.')
            print(pg_e)
            sys.exit(errors.EXIT_CODE_INVALID_SCHEMA)
        if 'This session does not have a current database.' in str(pg_e):
            print(
                f'The database provided either does not exist or your user does not have access to it. If no database was provided, this user does not have a default warehouse.')
            print(pg_e)
            sys.exit(errors.EXIT_CODE_INVALID_DATABASE)
        print(pg_e)
    except BaseException as e:
        if 'No such file or directory:' in str(e):
            print(
                f'The combination of folder name/file name that you provided ({source_full_path}) could not be found. Please check for typos and try again.')
            print(e)
            sys.exit(errors.EXIT_CODE_FILE_NOT_FOUND)
        print('Put method failed.')
        print(e)
        pass

    # If the put method fails, fall back to the original multi-insert
    # statement method.
    try:
        upload_data_with_insert(source_full_path,
                                table_name,
                                insert_method,
                                db_connection,
                                snowflake_data_types=snowflake_data_types)
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
            sys.exit(errors.EXIT_CODE_INVALID_WAREHOUSE)
        if 'session does not have a current database' in str(db_e):
            print(
                f'The database provided either does not exist or your user does not have access to it. If no database was provided, this user does not have a default warehouse.')
            print(db_e.orig)  # Avoids printing data to console
            sys.exit(errors.EXIT_CODE_INVALID_DATABASE)
        if 'is not recognized' in str(db_e):
            print(
                'One or more of the values in your file does not match data_type of its column.')
            print(db_e.orig)  # Avoids printing data to console
            sys.exit(errors.EXIT_CODE_INVALID_UPLOAD_VALUE)
        if 'This session does not have a current schema.' in str(db_e):
            print(f'The schema provided either does not exist or your user does not have access to it. If no schema was provided, no default schema exists.')
            print(db_e)
            sys.exit(errors.EXIT_CODE_INVALID_SCHEMA)
        print(f'Failed to upload file to Snowflake.')
        print(db_e.orig)  # Avoids printing data to console
        sys.exit(errors.EXIT_CODE_UNKNOWN_ERROR)
    except Exception as e:
        if 'C error: Expected' and 'fields' and 'saw' in str(e):
            print('File contents don\'t match the provided headers. One or more rows contain more columns than expected.')
            print(e)
            sys.exit(errors.EXIT_CODE_INVALID_UPLOAD_COLUMNS)
        print(f'Failed to upload file to Snowflake.')
        print(e)
        sys.exit(errors.EXIT_CODE_UNKNOWN_ERROR)
    print(f'{source_full_path} successfully uploaded to {table_name}.')


def main():
    args = get_args()
    source_file_name_match_type = args.source_file_name_match_type
    source_file_name = args.source_file_name
    source_folder_name = args.source_folder_name
    source_full_path = shipyard.files.combine_folder_and_file_name(
        folder_name=source_folder_name, file_name=source_file_name)
    table_name = args.table_name.upper()
    insert_method = args.insert_method
    data_types = args.snowflake_data_types
    if args.snowflake_data_types != '':
        data_types = ast.literal_eval(args.snowflake_data_types)
    else:
        data_types = None
    user_role = args.user_role if args.user_role != '' else None
    try:
        if args.private_key_path != '':
            if args.private_key_passphrase == '':
                print("Please provide a passphrase for your private key.")
                sys.exit(errors.EXIT_CODE_INVALID_ARGUMENTS)
            private_key = utils.decode_rsa(rsa_key=args.private_key_path, passphrase= args.private_key_passphrase)
            con = snowflake.connector.connect(user=args.username, account=args.account,
                                            warehouse=args.warehouse,
                                            database=args.database, schema=args.schema,
                                            role = user_role,
                                            private_key=private_key)
        
            db_connection = create_engine('snowflake://', creator = lambda: con)
        
        else:
            if args.user_role != '':
                db_connection = create_engine(URL(
                    account=args.account,
                    user=args.username,
                    password=args.password,
                    database=args.database,
                    schema=args.schema,
                    warehouse=args.warehouse,
                    role = user_role,
                ))
            else:
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
            sys.exit(errors.EXIT_CODE_INVALID_CREDENTIALS)
        print(db_e)
        sys.exit(errors.EXIT_CODE_UNKNOWN_ERROR)
    except DBAPIError as dbapi_e:
        if 'Verify the account name is correct' in str(dbapi_e):
            if '.' not in args.account:
                print(
                    f'Invalid account name. Instead of {args.account}, it might need to be something like {args.account}.us-east-2.aws, including the region.')
            else:
                print(
                    f'Invalid account name. Instead of {args.account}, it might need to be something like {args.account.split(".")[0]}, without the region.')
            print(dbapi_e)
            sys.exit(errors.EXIT_CODE_INVALID_ACCOUNT)
        print(dbapi_e)
        sys.exit(errors.EXIT_CODE_UNKNOWN_ERROR)
    except Exception as e:
        if 'quote_from_bytes() expected bytes' in str(e):
            print(f'The schema provided either does not exist or your user does not have access to it. If no schema was provided, no default schema exists.')
            sys.exit(errors.EXIT_CODE_INVALID_SCHEMA)
        print(f'Failed to connect to Snowflake.')
        print(e)
        sys.exit(errors.EXIT_CODE_UNKNOWN_ERROR)

    if source_file_name_match_type == 'regex_match':
        file_names = shipyard.files.find_all_local_file_names(
            source_folder_name)
        matching_file_names = shipyard.files.find_all_file_matches(
            file_names, re.compile(source_file_name))
        print(f'{len(matching_file_names)} files found. Preparing to upload...')

        for index, key_name in enumerate(matching_file_names):
            snowflake_results = upload_data(
                source_full_path=key_name,
                table_name=table_name,
                insert_method=insert_method,
                db_connection=db_connection,
                snowflake_data_types=data_types)

    else:
        snowflake_results = upload_data(
            source_full_path=source_full_path,
            table_name=table_name,
            insert_method=insert_method,
            db_connection=db_connection,
            snowflake_data_types=data_types)

    db_connection.dispose()


if __name__ == '__main__':
    main()

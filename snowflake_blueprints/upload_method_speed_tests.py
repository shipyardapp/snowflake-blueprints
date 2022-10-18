from snowflake.connector.pandas_tools import pd_writer, write_pandas
import pandas as pd
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import argparse
import shipyard_utils as shipyard
import time
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv
import dask.dataframe as dd

import warnings
warnings.simplefilter(action='ignore', category=UserWarning)
warnings.filterwarnings(
    action='ignore',
    message='Dialect snowflake:snowflake will not make use of SQL compilation caching.*')

DATA_SIZE = '50MB_WIDE'


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
    parser.add_argument('--source-file-name', dest='source_file_name',
                        default='output.csv', required=True)
    parser.add_argument('--source-folder-name',
                        dest='source_folder_name', default='', required=False)
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


def start_timer(method_name):
    print(f'Starting method {method_number}')
    start = time.perf_counter()
    print(start)
    return start


def end_timer(start, method_name):
    finish = time.perf_counter()
    print(finish)
    print(
        f'It took {finish - start} seconds to upload {DATA_SIZE} of data with method_{method_number}.')
    return finish


def create_table(source_full_path, table_name, db_connection):
    try:
        chunksize = 10000
        for index, chunk in enumerate(
                pd.read_csv(source_full_path, chunksize=chunksize)):
            chunk.head(0).to_sql(
                table_name,
                con=db_connection,
                if_exists="fail",
                index=False)
            # prevents a loop that was necessary for loading only a small chunk
            # of the data in.
            break
    except BaseException as e:
        print(e)


def method_csv_put_copy(source_full_path,
                        table_name,
                        insert_method,
                        db_connection):
    # PUT/COPY INTO CSV

    ###
    # 7KB = 4.888909576
    # 10MB = 11.80317694
    # 50MB Deep = 26.379184550999998
    # 50MB Wide = 32.249979941
    # 1GB Deep = 289.085156788
    # 1GB Wide = 302.586698846
    ###
    start = start_timer('put_copy_csv')
    if insert_method == 'replace':
        db_connection.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        print('table was dropped')
        create_table(source_full_path, table_name, db_connection)
        db_connection.execute(
            f'PUT file://{source_full_path} @%"{table_name}"')
        print('file was put')
        db_connection.execute(
            f'copy into "{table_name}" FILE_FORMAT=(type=csv,SKIP_HEADER=1) PURGE=TRUE')
        print('file was copied')
    elif insert_method == 'append':
        create_table(source_full_path, table_name, db_connection)
        db_connection.execute(
            f'PUT file://{source_full_path} @%"{table_name}"')
        db_connection.execute(
            f'copy into "{table_name}" FILE_FORMAT=(type=csv,SKIP_HEADER=1) PURGE=TRUE')
    finish = end_timer(start, 'put_copy_csv')


def method_pandas_parquet_put_copy(source_full_path,
                                   table_name,
                                   insert_method,
                                   db_connection):
    # Convert CSV to Parquet using Pandas Chunks
    # PUT/COPY INTO Parquet

    ###
    # 7KB = 3.9981184160000005
    # 10MB = 10.416837076
    # 50MB Deep = 30.421795795 (create), 76.227652442 (append)
    # 50MB Wide = 173.483919561, 165.565015879 (append)
    # 1GB Deep = 496.87234893600004
    # 1GB Wide = 2625.591407008
    ###
    start = start_timer('pandas_parquet_put_copy')

    chunksize = 10000
    for index, chunk in enumerate(
            pd.read_csv(source_full_path, chunksize=chunksize)):
        chunk.to_parquet(
            f'/tmp/method_pyarrow_parquet_put_copy_{index}.parquet')

    # code.interact(local=locals())
    if insert_method == 'replace':
        db_connection.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        print('table was dropped')
        create_table(source_full_path, table_name, db_connection)
        db_connection.execute(
            f'PUT file:///tmp/method_pandas_parquet_put_copy_* @%"{table_name}"')
        print('file was put')
        db_connection.execute(
            f'copy into "{table_name}" FILE_FORMAT=(type=PARQUET) PURGE=TRUE MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE')
        print('file was copied')
    elif insert_method == 'append':
        create_table(source_full_path, table_name, db_connection)
        db_connection.execute(
            f'PUT file:///tmp/method_pandas_parquet_put_copy_* @%"{table_name}"')
        db_connection.execute(
            f'copy into "{table_name}" FILE_FORMAT=(type=PARQUET) PURGE=TRUE MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE')
    finish = end_timer(start, 'pandas_parquet_put_copy')


def method_pyarrow_parquet_put_copy(source_full_path,
                                    table_name,
                                    insert_method,
                                    db_connection):
    # Convert CSV to Parquet using PyArrow
    # PUT/COPY INTO Parquet

    ###
    # 7KB = 4.095587211
    # 10MB = 11.607326992999997
    # 50MB Deep = 31.844763085000004 (create), 37.04284200000001 (append)
    # 50MB Wide = 116.12031062999998 (create), 113.87856825300003 (append)
    # 1GB Deep = 431.47511402299995
    # 1GB Wide = 2012.2350292660003
    ###
    start = start_timer('pyarrow_parquet_put_copy')

    writer = None
    with pyarrow.csv.open_csv(source_full_path) as reader:
        for next_chunk in reader:
            if next_chunk is None:
                break
            if writer is None:
                writer = pq.ParquetWriter(
                    '/tmp/method_pyarrow_parquet_put_copy_.parquet',
                    next_chunk.schema)
            next_table = pa.Table.from_batches([next_chunk])
            writer.write_table(next_table)
    writer.close()

    # code.interact(local=locals())
    if insert_method == 'replace':
        db_connection.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        print('table was dropped')
        create_table(source_full_path, table_name, db_connection)
        db_connection.execute(
            f'PUT file:///tmp/method_pyarrow_parquet_put_copy_* @%"{table_name}"')
        print('file was put')
        db_connection.execute(
            f'copy into "{table_name}" FILE_FORMAT=(type=PARQUET) PURGE=TRUE MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE')
        print('file was copied')
    elif insert_method == 'append':
        create_table(source_full_path, table_name, db_connection)
        db_connection.execute(
            f'PUT file:///tmp/method_pyarrow_parquet_put_copy* @%"{table_name}"')
        db_connection.execute(
            f'copy into "{table_name}" FILE_FORMAT=(type=PARQUET) PURGE=TRUE MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE')
    finish = end_timer(start, 'pyarrow_parquet_put_copy')


def method_dask_parquet_put_copy(source_full_path,
                                 table_name,
                                 insert_method,
                                 db_connection):
    # Convert CSV to Parquet using Dask
    # PUT/COPY INTO Parquet

    ###
    # 7KB = 3.813341257000001
    # 10MB = 8.039944266
    # 50MB Deep = 31.955688065000004(create), 30.793063626999995(append)
    # 50MB Wide = 107.87239970400003 (create), 106.80971107299996 (append)
    # 1GB Deep = 350.3969153579999
    # 1GB Wide = 799.4128316380002
    ###
    start = start_timer('dask_parquet_put_copy')

    df = dd.read_csv(source_full_path)
    df.to_parquet(f'/tmp/method_dask_parquet_put_copy', write_index=False)

    # code.interact(local=locals())
    if insert_method == 'replace':
        db_connection.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        print('table was dropped')
        create_table(source_full_path, table_name, db_connection)
        db_connection.execute(
            f'PUT file:///tmp/method_dask_parquet_put_copy/* @%"{table_name}"')
        print('file was put')
        db_connection.execute(
            f'copy into "{table_name}" FILE_FORMAT=(type=PARQUET) PURGE=TRUE MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE')
        print('file was copied')
    elif insert_method == 'append':
        create_table(source_full_path, table_name, db_connection)
        db_connection.execute(
            f'PUT file:///tmp/method_dask_parquet_put_copy/* @%"{table_name}"')
        db_connection.execute(
            f'copy into "{table_name}" FILE_FORMAT=(type=PARQUET) PURGE=TRUE MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE')
    finish = end_timer(start, 'dask_parquet_put_copy')


def method_pandas_tosql_pdwriter(source_full_path,
                                 table_name,
                                 insert_method,
                                 db_connection):
    start = start_timer('pandas_tosql_pdwriter')
    # Existing to_sql method with 'multi' insert statements

    ###
    # 7KB = 3.944108098000001
    # 10MB = 79.291645214
    # 50MB Deep = 475.389128133
    # 50MB Wide = 39.568710872
    # 1GB Deep = 8904
    # 1GB Wide = 563.1573168249997
    ###
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
        chunk.to_sql(
            table_name,
            con=db_connection,
            index=False,
            if_exists=insert_method,
            method=pd_writer,
            chunksize=10000)
    finish = end_timer(start, 'pandas_tosql_pdwriter')


def method_pandas_tosql_multi(source_full_path,
                              table_name,
                              insert_method,
                              db_connection):

    # Existing to_sql method with 'multi' insert statements

    ###
    # 7KB = 2.456918696999999
    # 10MB = 80.00960492200001
    # 50MB Deep = 477.0491879859999
    # 50MB Wide = 234.146332062
    # 1GB = DNT
    # 1GB Wide = DNF
    ###
    start = start_timer('pandas_tosql_multi')
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
        chunk.to_sql(
            table_name,
            con=db_connection,
            index=False,
            if_exists=insert_method,
            method='multi',
            chunksize=10000)
    finish = end_timer(start, 'pandas_tosql_multi')


def main():
    args = get_args()
    source_file_name = args.source_file_name
    source_folder_name = args.source_folder_name
    source_full_path = shipyard.files.combine_folder_and_file_name(
        folder_name=source_folder_name, file_name=source_file_name)
    print(source_full_path)
    insert_method = args.insert_method

    db_connection = create_engine(URL(
        account=args.account,
        user=args.username,
        password=args.password,
        database=args.database,
        schema=args.schema,
        warehouse=args.warehouse
    )).execution_options(autocommit=True)
    db_connection.connect()

    method_csv_put_copy(source_full_path=source_full_path,
                        table_name=f'METHOD_CSV_PUT_COPY_{DATA_SIZE}',
                        insert_method=insert_method,
                        db_connection=db_connection)
    method_pandas_parquet_put_copy(
        source_full_path=source_full_path,
        table_name=f'METHOD_PANDAS_PARQUET_PUT_COPY_{DATA_SIZE}',
        insert_method=insert_method,
        db_connection=db_connection)
    method_pyarrow_parquet_put_copy(
        source_full_path=source_full_path,
        table_name=f'METHOD_PYARROW_PARQUET_PUT_COPY_{DATA_SIZE}',
        insert_method=insert_method,
        db_connection=db_connection)
    method_dask_parquet_put_copy(
        source_full_path=source_full_path,
        table_name=f'METHOD_DASK_PARQUET_PUT_COPY_{DATA_SIZE}',
        insert_method=insert_method,
        db_connection=db_connection)
    method_pandas_tosql_pdwriter(
        source_full_path=source_full_path,
        table_name=f'METHOD_PANDAS_TOSQL_PDWRITER_{DATA_SIZE}',
        insert_method=insert_method,
        db_connection=db_connection)
    method_pandas_tosql_multi(
        source_full_path=source_full_path,
        table_name=f'METHOD_PANDAS_TOSQL_MULTI_{DATA_SIZE}',
        insert_method=insert_method,
        db_connection=db_connection)


if __name__ == '__main__':
    main()

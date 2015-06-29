#!/usr/bin/env python
#
from __future__ import print_function

import datetime
import getpass
import os
import shelve
import sys
import argparse

from textwrap import dedent

import psycopg2
import psycopg2.extras

## connect to db
def connect(autocommit=True):
    user = os.environ['USER']
    #password = getpass.getpass()
    conn = psycopg2.connect(dbname='app', host='/var/run/postgresql', port=5432)
    conn.autocommit = autocommit;
    conn.set_client_encoding('sql_ascii')
    print('Connected to {0}: encoding {1}'.format(conn.dsn, conn.encoding))
    return conn

## check if parent table
def is_parent_table(conn, schema, table):
    full_table_name = schema + '.' + table
    query = '''select true from pg_inherits where inhparent = %s::regclass limit 1'''
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        cursor.execute(query, (full_table_name, ))
        result = cursor.fetchone()
        return result

## get char-based columns
def char_based_columns(conn, schema, table):
    columns_and_types = {}
    query = dedent( '''
                    SELECT pg_attribute.attname AS column_name,
                           format_type(pg_attribute.atttypid, pg_attribute.atttypmod) as column_type
                      FROM pg_class
                      JOIN pg_namespace ON (pg_class.relnamespace = pg_namespace.oid)
                      JOIN pg_attribute ON (pg_attribute.attrelid = pg_class.oid)
                      JOIN pg_type ON (pg_attribute.atttypid = pg_type.oid)
                     WHERE pg_class.relkind = 'r'
                       AND pg_type.typtype = 'b'
                       AND pg_type.typcategory = 'S'
                       AND NOT pg_attribute.attisdropped
                       AND pg_attribute.attnum > 0
                       AND pg_namespace.nspname = quote_ident(%s)
                       AND pg_class.relname = quote_ident(%s)
                     ORDER BY pg_attribute.attnum''')
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        cursor.execute(query, (schema, table, ))
        for row in cursor:
            columns_and_types[row['column_name']] = row['column_type']
    return columns_and_types

## get shortest unique key columns for table
def get_unique_key_columns(conn, schema, table):
    keys = []
    datatypes = []
    query = dedent('''
                    select out_unique_key_col,
                           out_unique_key_data_type
                      from get_shortest_unique_key(%s, %s)''')
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        cursor.execute(query, (schema, table, ))
        results = cursor.fetchall()
        [keys.extend(row['out_unique_key_col']) for row in results]
        [datatypes.extend(row['out_unique_key_data_type']) for row in results]

    return (keys, datatypes)

## main
if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--schema', dest='schema', action='store', required=True,
                        help='schema name for table - required')
    parser.add_argument('--table', dest='table', action='store', required=True,
                        help='table name - required')
    parser.add_argument('--update', dest='update', action='store_true', required=False,
                        help='update table')
    parser.add_argument('--debug', dest='debug', action='store_true', required=False,
                        help='print debug info')
    args = parser.parse_args()

    ## connect, get char-based columns, keys and key datatypes
    read_conn = connect(autocommit=False)
    write_conn = connect()

    if is_parent_table(read_conn, args.schema, args.table):
	print ("{0}.{1} is a parent table.  Skipping check on this table, as all children will be checked individually.".format(args.schema, args.table))
        sys.exit(0)

    columns_and_types = char_based_columns(read_conn, args.schema, args.table)
    columns = columns_and_types.keys()
    keys, datatypes = get_unique_key_columns(read_conn, args.schema, args.table)

    ## create select query using above
    all_columns = []
    all_columns.extend(columns)
    all_columns.extend(keys)

    select_query = 'select {0} from {1}.{2} order by {3}'.format(', '.join(all_columns),
                                                                 args.schema, args.table,
                                                                 ', '.join(keys))

    ## open query log
    start = datetime.datetime.now()
    query_log = '{0}.{1}-queries.{2}'.format(args.schema, args.table, start.strftime('%Y-%m-%d-%H-%M-%S'))
    queries = open(query_log, 'wb')

    try:
        stmt_counter = 0
        with read_conn.cursor(name='read_cursor', cursor_factory=psycopg2.extras.RealDictCursor) as read_cursor:
            print ('Select query: ', select_query)
            read_cursor.execute(select_query)
            rows = read_cursor.fetchmany(5000)
            while rows:
                for row in rows:
                    updates = {}
                    for column in columns:
                        if row[column]:
                            try:
                                recoded = row[column].decode('utf-8')
                            except UnicodeDecodeError:
                                updates[column] = row[column].decode('latin1').encode('utf-8')
                    if updates:
                        with write_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as write_cursor:
                            update_cols = sorted(updates.keys())
                            ## create update query
                            update_query  = ('update {0}.{1} '
                                             '   set {2} '
                                             ' where {3} ;\n'.format(args.schema, args.table,
                                            ', '.join('{0} = %s::{1}'.format(c, columns_and_types[c]) for c in update_cols),
                                            ' and '.join('{0} = %s'.format(k) for k in keys)))
                            update_values = [updates[col] for col in update_cols]
                            update_values.extend([row[k] for k in keys])
                            stmt = write_cursor.mogrify(update_query, update_values)
                            if (args.debug):
                                print (stmt)
                            queries.writelines(stmt)
                            stmt_counter += 1

                            try:
                                if (args.update):
                                    write_cursor.execute(update_query, update_values)
                            except psycopg2.Error, e:
                                print ('PG error code: {0}'.format(e.pgcode))
                                print (e.pgerror)
                            finally:
                                write_cursor.close()

                rows = read_cursor.fetchmany(5000)
        fini = datetime.datetime.now()
	td = fini - start
	total_seconds = 1.0 * (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 10**6

        if (args.update):
            print('Updated {0} rows in {1} seconds.'.format(
              stmt_counter, total_seconds)) # (fini - start).total_seconds()))
        else:
            print('Generated {0} update statements in {1} seconds.'.format(
                  stmt_counter, total_seconds)) #(fini - start).total_seconds()))

    except psycopg2.Error, e:
        print ('PG error code: {0}'.format(e.pgcode))
        print (e.pgerror)

    finally:
        queries.close()
        write_conn.close()
        read_conn.close()


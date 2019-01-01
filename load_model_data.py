import sys
import re
import psycopg2
import hindcast_common
from io import StringIO
import datetime

def year_plus_hours(year, hours):
    return datetime.datetime(year, 1, 1) + datetime.timedelta(hours=hours)

def db_prepare_record_data(time, node_id, value):
    """
    Prepare to write record data to database
    """
    return str(node_id) + '\t' + str(time) + '\t' + str(value) + '\n'

def md_table_name(md):
    return md.get('var', '').lower() + '_' + md.get('year', '')

def db_copy_record_data(md, conn, dbdata):
    f = StringIO(unicode(dbdata))
    cur = conn.cursor()
    cur.copy_from(f, md_table_name(md), columns=('node_id', 't', 'val'))

def parse_write_default_data(default_value, start_id, end_id, time):
    """
    Fill in default value for missing node_id range
    """
    dbdata = ''
    for id in range(start_id, end_id):
        dbdata += db_prepare_record_data(time, id, default_value)
    return dbdata

def parse_write_record(md, conn, file, time):
    """
    Parse and write a record
    """
    t = tuple(re.split(r'\s+', file.readline().strip()))
    time_step = long(float(t[0]))
    num_data_pts = t[2]
    default_value = t[3]
    node_id = 0
    dbdata = ''
    for r in range( int(num_data_pts) ):
        old_node_id = node_id
        t = tuple(re.split(r'\s+', file.readline().strip()))
        node_id = int(t[0])
        value = t[1]
        if old_node_id >= node_id:
            print('ERROR: node numbers are not sequential!')
            sys.exit(1)
        if node_id != old_node_id + 1:
            dbdata += parse_write_default_data(default_value, old_node_id + 1,
                                               node_id, time)
        dbdata += db_prepare_record_data(time, node_id, value)
    db_copy_record_data(md, conn, dbdata)

def parse_write_model_data(md, conn):
    """
    Parse and write model data to database
    """
    file = open(md.get('file'), 'r')
    ignore = file.readline()
    t = tuple(re.split(r'\s+', file.readline().strip()))
    num_time_steps = t[0]
    num_grid_pts = t[1]
    time_step = t[2]
    hour = 1
    for r in range( int(num_time_steps) ):
        if r >= 744 or int(md.get('year', '0')) == 1980:
            time = year_plus_hours(int(md.get('year', 0)), hour)
#            print('Parsing and writing record ' + str(r) +
#                  ' (hour ' + str(hour) + ') ' + str(time))
            parse_write_record(md, conn, file, str(time))
            hour += 1

def create_table(md, conn):
    """
    Create table for model data in database
    """
    sql = ("create table " + md_table_name(md) + " (" +
           "node_id integer not null, " +
           "t timestamp with time zone not null, " +
           "val double precision not null);")
    cur = conn.cursor()
    cur.execute(sql)
    cur.close()

def create_index(md, conn, columns):
    """
    Create specified index in database
    """
    sql = ("create index on " + md_table_name(md) +
           " (" + ", ".join(columns) + ");")
    cur = conn.cursor()
    cur.execute(sql)
    cur.close()

def create_indexes(md, conn):
    """
    Create required indexes on model data table.
    """
#    print('Indexing 1 of 3')
    create_index(md, conn, ['node_id', 't', 'val'])
#    print('Indexing 2 of 3')
#    create_index(md, conn, ['t', 'node_id', 'val'])
#    print('Indexing 3 of 3')
#    create_index(md, conn, ['val', 't', 'node_id'])

def load_model_data(md):
    """
    Read and parse model data from file and write to database
    """
    conn = hindcast_common.db_connect()
    try:
        print('Creating table ' + md_table_name(md))
        create_table(md, conn)
        print('Reading model data file and loading to database')
        parse_write_model_data(md, conn)
        print('Creating indexes')
        create_indexes(md, conn)
        print('Committing')
        conn.commit()
        print('OK')
    finally:
        conn.close()

def is_int(s):
    try:
        int(s)
        return True
    except ValueError:
        return False

if __name__ == '__main__':
    if (len(sys.argv) < 4 or len(sys.argv[2]) != 4 or
            is_int(sys.argv[2]) == False):
        print('Usage:  python load_model_data.py [variable] [year] [file_path]')
        print(' e.g.:  python load_model_data.py tmm10 1999 swan_TMM10.63')
        sys.exit(1)
    md = {'var': sys.argv[1], 'year': sys.argv[2], 'file': sys.argv[3]}
    load_model_data(md);


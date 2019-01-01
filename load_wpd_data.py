import sys
import psycopg2
import hindcast_common

def table_name(year):
    return 'wpd_' + year

def create_table(conn, table):
    """
    Create table for wpd in database
    """
    sql = ("create table " + table + " (" +
           "node_id integer not null, " +
           "t timestamp with time zone not null, " +
           "hs double precision not null, " +
           "tmm10 double precision not null, " +
           "wpd double precision not null);")
    cur = conn.cursor()
    cur.execute(sql)
    cur.close()

def create_index(conn, table, columns):
    """
    Create specified index in database
    """
    sql = ("create index on " + table +
           " (" + ", ".join(columns) + ");")
    cur = conn.cursor()
    cur.execute(sql)
    cur.close()

def create_indexes(conn, table):
    """
    Create required indexes on model data table.
    """
    print('Indexing 1 of 5')
    create_index(conn, table, ['node_id', 't', 'wpd', 'tmm10', 'hs'])
    print('Indexing 2 of 5')
    create_index(conn, table, ['t', 'node_id', 'wpd', 'tmm10', 'hs'])
    print('Indexing 3 of 5')
    create_index(conn, table, ['wpd', 'tmm10', 'hs', 't', 'node_id'])
    print('Indexing 4 of 5')
    create_index(conn, table, ['tmm10', 'wpd', 'hs', 't', 'node_id'])
    print('Indexing 5 of 5')
    create_index(conn, table, ['hs', 'wpd', 'tmm10', 't', 'node_id'])

def write_wpd_data(conn, table, year):
    """
    Write data to WPD table.
    """
    sql = ("insert into " + table +
           " select tmm10.node_id node_id, tmm10.t t," +
           " hs.val hs," +
           " tmm10.val tmm10," +
           " (490.0 * tmm10.val * power(hs.val, 2.0)) wpd" +
           "  from tmm10_" + year + " tmm10" +
           "   join hs_" + year + " hs" +
           "    on tmm10.node_id = hs.node_id and tmm10.t = hs.t;")
    cur = conn.cursor()
    cur.execute(sql)
    cur.close()

def load_wpd_data(year):
    """
    Load wpd model data to database
    """
    conn = hindcast_common.db_connect()
    table = table_name(year)
    try:
        print('Creating table ' + table)
        create_table(conn, table)
        print('Loading WPS model data to database')
        write_wpd_data(conn, table, year)
        print('Creating indexes')
        create_indexes(conn, table)
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
    if (len(sys.argv) < 2 or len(sys.argv[1]) != 4 or
            is_int(sys.argv[1]) == False):
        print('Usage:  python load_wpd_data.py [year]')
        print(' e.g.:  python load_wpd_data.py 1999')
        sys.exit(1)
    load_wpd_data(sys.argv[1]);


import sys
import stat
import os
import psycopg2
import ConfigParser

def postgres_connect(dbname, user, password, host, port):
    """
    Connect to Postgres database.
    """
    conn = psycopg2.connect('dbname=' + dbname +
                            ' user=' + user +
                            ' password=' + password +
                            ' host=' + host +
                            ' port=' + port)
    conn.set_session(isolation_level='SERIALIZABLE')
    return conn

def accessible_by_group_or_world(file):
    """
    Check if file has secure permissions for database password.
    """
    st = os.stat(file)
    return bool( st.st_mode & (stat.S_IRWXG | stat.S_IRWXO) )

def get_config():
    """
    Read config file.
    """
    config_file = os.environ['HOME'] + '/.hindcast'
    if accessible_by_group_or_world(config_file):
        print ('ERROR: config file ' + config_file + ' has group or world ' +
        'access; permissions should be set to u=rw')
        sys.exit(1)
    config = ConfigParser.RawConfigParser()
    config.read(config_file)
    return config

def db_connect():
    """
    Connect to hindcast database.
    """
    config = get_config()
    conn = postgres_connect(config.get('default', 'dbname'),
                            config.get('default', 'user'),
                            config.get('default', 'password'),
                            config.get('default', 'host'),
                            config.get('default', 'port'))
    return conn

if __name__ == '__main__':
    print('This is a function library for hindcast tools.')


import sys
import re
import psycopg2
import hindcast_common

input_grid_file = 'data/fort.14'

srid = 4326

def parse_grid_file():
    """
    Read and parse grid from file.
    """
    file = open(input_grid_file, 'r')
    agrid = file.readline().strip()
    (ne, np) = tuple(re.split(r'\s+', file.readline().strip()))
    # Read nodes
    nodes = int(np) * [None]
    for k in range(int(np)):
        (jn, x, y, dp) = tuple(re.split(r'\s+', file.readline().strip()))
        nodes[int(jn) - 1] = (jn, x, y, dp)
    # Read elements
    elements = int(ne) * [None]
    for k in range(int(ne)):
        (je, nhy, nm1, nm2, nm3) = tuple(
            re.split(r'\s+', file.readline().strip()) )
        elements[int(je) - 1] = (je, nm1, nm2, nm3)
    file.close()
    return (nodes, elements)

def db_insert_grid_element(conn, element):
    """
    Write grid elements (triangles) to database.
    """
    (je, nm1, nm2, nm3) = element
    sql = ("insert into grid_element " +
           "(element_id, geom) " +
           "values (%s, " +
           "ST_GeometryFromText('POLYGON((%s %s, %s %s, %s %s, %s %s))', " +
                                str(srid) + ")" +
           ");")
    cur = conn.cursor()
    cur.execute(sql, (je,
                      float(nm1[1]), float(nm1[2]),
                      float(nm2[1]), float(nm2[2]),
                      float(nm3[1]), float(nm3[2]),
                      float(nm1[1]), float(nm1[2]) )
               )

def db_insert_grid_node(conn, node):
    """
    Write grid nodes (points and depths) to database.
    """
    (jn, x, y, dp) = node
    sql = ("insert into grid_node " +
           "(node_id, depth, geom) " +
           "values (%s, %s, " +
           "ST_GeometryFromText('POINT(%s %s)', " + str(srid) + ")" +
           ");")
    cur = conn.cursor()
    cur.execute(sql, (jn, dp, float(x), float(y)) )

def write_grid(conn, nodes, elements):
    """
    Write grid to database.
    """
    for element in elements:
        je = element[0]
        nm1 = nodes[int(element[1]) - 1]
        nm2 = nodes[int(element[2]) - 1]
        nm3 = nodes[int(element[3]) - 1]
        db_insert_grid_element(conn, (je, nm1, nm2, nm3) )
    for node in nodes:
        db_insert_grid_node(conn, node)

def load_grid():
    """
    Read and parse grid from file and write to database.
    """
    print('Reading grid file')
    (nodes, elements) = parse_grid_file()
    print('Loading grid to database')
    conn = hindcast_common.db_connect()
    try:
        write_grid(conn, nodes, elements)
        conn.commit()
        print('OK')
    finally:
        conn.close()

if __name__ == '__main__':
    load_grid();


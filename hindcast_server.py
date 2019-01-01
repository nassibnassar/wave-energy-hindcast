import os
from datetime import datetime
from time import strftime
from flask import Flask, request, send_file
from flask.ext.runner import Runner
import tempfile
import zipfile
from hindcast_common import db_connect

temp_directory = '/data4/hindcast/'

app = Flask(__name__)
runner = Runner(app)

def write_result(cur, linebreak, separator):
    """
    Write result of database query in PSV.
    """
    # Write column names.
    attrs = [a[0] for a in cur.description]
    header = separator.join(attrs) + linebreak
    # Write data.
    result = cur.fetchall()
    rows = []
    for row in result:
        fields = []
        for field in row:
            fields.append(str(field))
        rows.append(separator.join(fields))
    all = linebreak.join(rows)
    return header + all + linebreak

def run_query(operation, parameters=None, linebreak='\r\n', separator='|'):
    """
    Execute database query and return results in PSV.
    """
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(operation, parameters)
    out = write_result(cur, linebreak, separator)
    cur.close()
    conn.close()
    return out

def db_query(operation, parameters=None, format=None):
    if format == 'zip':
        return run_query(operation, parameters, '\n', '\t')
    else:
        return run_query(operation, parameters)

@app.route('/')
def index():
    s = ('<pre>' +
         'Available services:\r\n' +
         '\r\n' +
         '/query_rectangle?ATTRIBUTE=VALUE&ATTRIBUTE=VALUE&...\r\n' +
         '    with the following ATTRIBUTE/VALUE pairs:\r\n' +
         '    ullat=ULLAT (required)\r\n' +
         '        where ULLAT is the upper left lat coordinate\r\n' +
         '    ullong=ULLONG (required)\r\n' +
         '        where ULLONG is the upper left long coordinate\r\n' +
         '    lrlat=LRLAT (required)\r\n' +
         '        where LRLAT is the lower right lat coordinate\r\n' +
         '    lrlong=LRLONG (required)\r\n' +
         '        where LRLONG is the lower right long coordinate\r\n' +
         '    tmin=TMIN (required)\r\n' +
         '        where TMIN is the start time, the minimum value of t\r\n' +
         '    tmax=TMAX (required)\r\n' +
         '        where TMAX is the end time, the maximum value of t\r\n' +
         '    fn=avg (optional)\r\n' +
         '        where avg indicates average data values for each node\r\n' +
         '    format=zip (optional)\r\n' +
         '        where zip specifies that results should be returned in\r\n' +
         '                  a zipped, tab-delimited file, rather than the\r\n' +
         '                  default PSV.\r\n' +
         '    e.g.: <a href="/query_rectangle?ullat=35.236863&' +
                    'ullong=-75.158581&lrlat=34.736361&lrlong=-75.024660&' +
                    'tmin=1990-12-01&tmax=1991-02-01&fn=avg" target="_blank">' +
                    '/query_rectangle?ullat=35.236863&ullong=-75.158581&' +
                    'lrlat=34.736361&lrlong=-75.024660&' +
                    'tmin=1990-12-01&tmax=1991-02-01&fn=avg</a>\r\n' +
         '</pre>\n')
    return s

def zip_data(data):
    user_file_name = ('hindcast-' +
                     datetime.now().strftime('%Y%m%d-%H%M%S-%f'))
    file = tempfile.NamedTemporaryFile(prefix='hindcast-',
                                       dir=temp_directory)
    file.write(data)
    file.flush()
    zip_name = file.name + '-zip'
    zip = zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED)
    zip.write(file.name, user_file_name + '.txt')
    zip.close()
    return send_file(zip_name, mimetype='application/zip',
                     as_attachment=True,
                     attachment_filename=user_file_name+'.zip')

def return_data(data, format):
    if format == 'zip':
        return zip_data(data)
    else:
        return '<pre>\r\n' + data + '</pre>\r\n'

@app.route('/query_rectangle', methods=['GET'])
def query_rectangle():
    ullat = float(request.args.get('ullat'))
    ullong = float(request.args.get('ullong'))
    lrlat = float(request.args.get('lrlat'))
    lrlong = float(request.args.get('lrlong'))
    tmin = str(request.args.get('tmin'))
    tmax = str(request.args.get('tmax'))
    fn = str(request.args.get('fn'))
    op = ("select node_id, " +
          "split_part(" +
          "trim( leading 'POINT(' from (" +
          "trim(trailing ')' from ST_AsText(geom))) ), ' ', 2)" +
          " lat, " +
          "split_part(" +
          "trim( leading 'POINT(' from (" +
          "trim(trailing ')' from ST_AsText(geom))) ), ' ', 1)" +
          " long, " +
          ("avg(hs) hs_avg, avg(tmm10) tmm10_avg, avg(wpd) wpd_avg "
              if fn == 'avg' else "t, hs, tmm10, wpd ") +
          "from grid_node " +
          "natural join wpd_all " +
          "where " + 
          "ST_Within( geom, ST_GeomFromText('" +
          "POLYGON((%s %s, %s %s, %s %s, %s %s, %s %s))', 4326) ) " +
          "and " +
          "t >= %s and t <= %s " +
          ("group by node_id "
              if fn == 'avg' else "") +
          ("order by node_id"
              if fn == 'avg' else "order by t, node_id") +
          ";")
    param = (ullong, ullat,
             lrlong, ullat,
             lrlong, lrlat,
             ullong, lrlat,
             ullong, ullat,
             tmin, tmax)
    print(op % param)
    format = request.args.get('format')
    out = db_query(op, param, format)
    return return_data(out, format)

if __name__ == '__main__':
    runner.debug = True
    runner.run(host='0.0.0.0')


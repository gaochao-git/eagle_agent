import os
import random
import re
import socket
import subprocess
import time

import pymysql


def get_hostname():
    return socket.gethostname()


def get_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip


def build_insert_sql(obj):
    assert isinstance(obj, dict)

    sql_template = 'insert into {db}.{table}({cols}) values ({vals}) on duplicate key update {dups}'

    cols = ''
    vals = ''
    dups = ''
    for col, val in obj['data'].items():
        cols = cols + col + ', '
        vals = vals + repr(val) + ', '
        dups = dups + col + '=' + repr(val) + ', '

    cols = cols + 'updated_time' + ', '
    vals = vals + 'CURRENT_TIMESTAMP' + ', '
    dups = dups + 'updated_time' + '=' + 'CURRENT_TIMESTAMP' + ', '

    return sql_template.format(
        db=obj['db'],
        table=obj['table'],
        cols=cols.strip(', '),
        vals=vals.strip(', '),
        dups=dups.strip(', '))


def build_mysql_conn(host, port, user, passwd):
    try:
        return pymysql.connect(
            host=host,
            port=port,
            user=user,
            passwd=passwd,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)
    except Exception as e:
        raise Exception('Can not build available connection!')
        return None


def build_local_conn(port):
    return build_mysql_conn('127.0.0.1', port, 'dba', 'xDvmgk67izldscTs')


def build_dc_conn():
    return build_mysql_conn('10.88.132.153', 3306, 'dba_agent',
                            'qsgXzYMkTx8WBcRE')


def is_multi_version_mysql():
    return False if os.path.exists(MYSQL_DIR + 'bin') else True


def get_instance_on_disk():
    if not os.path.exists(MYSQL_DIR + 'multi'):
        return []
    dirs = os.listdir(MYSQL_DIR + 'multi')
    port_pattern = re.compile('\d{4}$')
    out = []
    for directory in dirs:
        if port_pattern.match(directory):
            out.append(int(directory))
    return out


def write_to_datacenter(*obj):
    with build_dc_conn() as cursor:
        for item in obj:
            cursor.execute(build_insert_sql(item))


def get_mysql_version(cursor):
    cursor.execute('show variables like \'version\'')
    return cursor.fetchone()['Value'].split('-')[0]


def is_instance_running(port):
    try:
        build_local_conn(port)
        return True
    except Exception as e:
        return False


def try_build_local_conn(port):
    try:
        return build_local_conn(port)
    except Exception as e:
        return None


def byte_convert(size):
    ct = 0
    flt = False
    while size > 1024 and ct < 3:
        if size % 1024 != 0:
            flt = True
        size = size / 1024
        ct = ct + 1
    metric = None
    if ct == 0:
        metric = 'B'
    elif ct == 1:
        metric = 'K'
    elif ct == 2:
        metric = 'M'
    else:
        metric = 'G'
    size = size if flt else int(size)
    return str(round(size, 0)) + metric


def size_reverse(size):
    metric = size[-1]
    numeric = size[:-1]
    coefficient = 0
    if metric == 'B':
        coefficient = 1
    elif metric == 'K':
        coefficient = 1024
    elif metric == 'M':
        coefficient = 1024 ** 2
    elif metric == 'G':
        coefficient = 1024 ** 3
    elif metric == 'T':
        coefficient = 1024 ** 4
    else:
        numeric = size
        coefficient = 1
    number = float(numeric) * coefficient if '.' in numeric else int(
        numeric) * coefficient
    return number


def get_obj_size(path):
    return int(subprocess.getoutput('du -sb %s' % path).split('\t')[0])


def size_compare(size1, size2):
    if size_reverse(size1) < size_reverse(size2):
        return -1
    elif size_reverse(size1) == size_reverse(size2):
        return 0
    else:
        return 1


def timestamp_to_time(timestamp):
    time_struct = time.localtime(timestamp)
    return time.strftime('%Y-%m-%d %H:%M:%S', time_struct)


def get_file_info(path):
    size = os.path.getsize(path)
    atime = timestamp_to_time(os.path.getatime(path))
    mtime = timestamp_to_time(os.path.getmtime(path))
    return size, atime, mtime


#!/bin/envs python
#-*- coding:utf-8 -*-

import pymysql
import socket
import os
import re
import logging.handlers
logger = logging.getLogger('agent_logger')
# 获取本机mysql实例
def get_mysql_running_port():
    running_port_list = []
    pids = [pid for pid in os.listdir('/proc') if pid.isdigit()]
    for pid in pids:
        try:
            process_cmd_info_list = open(os.path.join('/proc', pid, 'cmdline'), 'rb').read().split(b'\0')
            match_cmd = process_cmd_info_list[0].decode('utf-8')
            if re.findall('(mysqld)',match_cmd) and re.findall('(--port)',process_cmd_info_list[-2].decode('utf-8')):
                running_port = process_cmd_info_list[-2].decode('utf-8').split('=')[-1]
                if running_port not in running_port_list:
                    running_port_list.append(running_port)
        except Exception as e:
            print(e)   #不需要打印到日志,因为有些进程号是瞬间的,会误导
    return running_port_list
                
# 连接本地mysql方法
def build_local_mysql_conn(port):
    try:
        return pymysql.connect(
            host='127.0.0.1',
            port=int(port),
            user='dba_agent',
            passwd='fffjjj',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor,
        )
    except Exception as e:
        print(e)
        logger.error(e)
        return None

# 获取主机名
def get_hostname():
    return socket.gethostname()

# 获取主机ip
def get_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip
# 获取mysql版本
def get_mysql_version(cursor):
    try:
        cursor.execute('show variables like "version"')
        return cursor.fetchone()['Value'].split('-')[0]
    except Exception as e:
        print(e)
        logger.error(e)
# 获取mysql server字符集
def get_mysql_character_set_server(cursor):
    try:
        cursor.execute('show variables like "character_set_server"')
        return cursor.fetchone()['Value']
    except Exception as e:
        print(e)
        logger.error(e)
# 获取innodb bufferpool
def get_innodb_buffer_pool_size(cursor):
    try:
        cursor.execute('show variables like "innodb_buffer_pool_size"')
        size = cursor.fetchone()['Value']
        return int(size)/1024/1024
    except Exception as e:
        logger.error(e)
# 获取read_only
def get_read_only(cursor):
    try:
        cursor.execute('show variables like "read_only"')
        return cursor.fetchone()['Value']
    except Exception as e:
        print(e)
        logger.error(e)
# 获取slave 信息
def get_slave_info(cursor):
    try:
        cursor.execute("show slave status")
        row = cursor.fetchone()
        if row is None:
            return '', ''
        elif row['Slave_IO_Running'].lower() != 'yes':
            return '', ''
        else:
            return row['Master_Host'], row['Master_Port']
    except Exception as e:
        print(e)
        logger.error(e)
# eagle_agent dml sql
def eagle_agent_dml_sql(sql):
    try:
        eagle_agent_connection = pymysql.connect(host='39.97.247.142', port=3306, user='wthong', password='fffjjj',database='eagle_agent', charset='utf8')
        cursor = eagle_agent_connection.cursor()
        cursor.execute(sql)
        eagle_agent_connection.commit()
    except Exception as e:
        eagle_agent_connection.rollback()
        print(e)
        logger.error(e)
    finally:
        cursor.close()
        eagle_agent_connection.close()
        
insert_instance_sql_column = "replace into eagle_agent.mysql_instance(host_name,host_ip,port,read_only,version,bufferpool,server_charset,master_ip,master_port,instance_status,instance_name)"
def main():
    host_ip = get_ip()
    host_name = get_hostname()
    port_list = get_mysql_running_port()
    for port in port_list:
        print(port)
        try:
            local_conn = build_local_mysql_conn(port)
            if not local_conn:
                sql = "replace into eagle_agent.mysql_instance(host_name,host_ip,port,instance_status) values('{}','{}','{}',3)".format(host_name,host_ip,port)
                eagle_agent_dml_sql(sql)
            else:
                with local_conn.cursor() as cursor:
                    slave_info_list = get_slave_info(cursor)
                    if slave_info_list:
                        master_ip = slave_info_list[0]
                        master_port = slave_info_list[1]
                    mysql_version = get_mysql_version(cursor)
                    mysql_character_set_server = get_mysql_character_set_server(cursor)
                    innodb_buffer_pool_size = get_innodb_buffer_pool_size(cursor)
                    read_only = get_read_only(cursor)
                    sql = insert_instance_sql_column + " values('{}','{}',{},'{}','{}',{},'{}','{}','{}',1,'{}_{}')".format(host_name,host_ip,port,read_only,mysql_version,innodb_buffer_pool_size,mysql_character_set_server,master_ip,master_port,host_ip,port)
                    eagle_agent_dml_sql(sql)
                local_conn.close()
        except Exception as e:
            print(str(e))
            logger.error(e)

if __name__ == '__main__':
    main()

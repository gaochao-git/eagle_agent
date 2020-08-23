#!/usr/bin/env python
# -*- coding: utf-8 -*-
# gaochao
import time
import os
import collections
import logging.handlers
import platform as pf

import psutil as ps
import pymysql as db
import requests
import yaml


with open('../config/eagle_agent.yml',mode='r') as f:
    eagle_agent_config = yaml.load(f,Loader=yaml.FullLoader)
    agent_db_connect_info = eagle_agent_config['eagle_db_test']

logger = logging.getLogger('agent_logger')

def conn_mysql_instance(host, port, user, password, database):
    try:
        return db.connect(
            host=host,
            port=port,
            user=user,
            passwd=password,
            db=database,
            charset='utf8mb4',
            cursorclass=db.cursors.DictCursor)
    except Exception as e:
        raise Exception('Can not build available connection!' + e)
        return None


def create_remote_mysql_conn():
    return conn_mysql_instance( agent_db_connect_info['mysql_ip'], agent_db_connect_info['mysql_port'], agent_db_connect_info['mysql_user'],agent_db_connect_info['mysql_pass'],agent_db_connect_info['mysql_db'])


def domain_is_valid(domain):
    if '.' in domain:
        d=domain.split('.')
        for i in d:
            for c in i:
                if not (ord(c) == 45 or 57 >= ord(c) >= 48 or 97 <= ord(c) <= 122 or ord(c) == 46):
                    return 0
    else:
        return 0

    return 1

# 获取操作系统基础信息
def get_os_info():
    os_info = collections.OrderedDict()
    # 操作系统基础信息
    os_uname = pf.uname()
    linux_distribution_tuple = pf.linux_distribution()
    os_info['linux_distribution'] = linux_distribution_tuple[0]+linux_distribution_tuple[1]
    os_info['os_system'] = os_uname.system
    os_info['host_name'] = os_uname.node
    os_info['os_machine'] = os_uname.release
    os_info['os_processor'] = os_uname.processor

    # 内存基础信息,大小单位为M
    memory = ps.virtual_memory()
    os_info['mem_total'] = round(memory.total / 1024 / 1024, 2)
    os_info['mem_available'] = round(memory.available / 1024 / 1024, 2)
    os_info['mem_used'] = round(memory.used / 1024 / 1024, 2)
    os_info['mem_free'] = round(memory.free / 1024 / 1024, 2)

    # 获取服务器CPU信息
    # CPU逻辑个数
    os_info['l_cpu_count'] = ps.cpu_count() if ps.cpu_count() else 0
    # CPU物理个数
    os_info['p_cpu_count'] = ps.cpu_count(logical=False) if ps.cpu_count(logical=False) else 0

    # 获取服务器开机时间
    os_info['boot_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(ps.boot_time())))
    return os_info


# 获取磁盘信息,大小单位为M
def get_disk_info():
    disk_partitions = ps.disk_partitions()
    all_disk_info = []
    for part in disk_partitions:
        disk_info = collections.OrderedDict()
        # 每个分区的路径
        disk_info['part_path'] = part.device
        # 每个分区的挂载点
        disk_info['part_mountpoint'] = part.mountpoint
        # 每个分区的总大小
        disk_info['part_total'] = round(ps.disk_usage(part.mountpoint).total / 1024 / 1024, 2)
        # 每个分区的已用大小
        disk_info['part_used'] = round(ps.disk_usage(part.mountpoint).used / 1024 / 1024, 2)
        # 每个分区的空闲大小
        disk_info['part_free'] = round(ps.disk_usage(part.mountpoint).free / 1024 / 1024, 2)
        # 每个分区使用百分比
        disk_info['part_usedper'] = ps.disk_usage(part.mountpoint).percent

        # 将所有分区的信息保存在列表中
        all_disk_info.append(disk_info)
    # 返回所有分区信息的列表
    return all_disk_info


# 获取网络相关信息
def get_network_info():
    all_network_info = []
    net_addrs = ps.net_if_addrs()
    for net_key, net_value in net_addrs.items():
        network_info = collections.OrderedDict()
        network_info['net_card_name'] = net_key
        for value_item in net_value:
            if value_item.family == 2 and not value_item.address == '127.0.0.1' and not value_item.netmask == '255.255.255.255':
                net_speed = os.popen(
                    "sudo ethtool " + net_key + "| grep Speed | awk -F ':' '{print $2}'").readline().strip()
                network_info['net_addr_ip'] = value_item.address
                network_info['net_speed'] = net_speed
                all_network_info.append(network_info)
    return all_network_info

def collect_all_info():
    # 获取各项基础信息
    os_info = get_os_info()
    host_name = os_info['host_name']
    disk_info = get_disk_info()
    network_info = get_network_info()
    print(os_info)
    print(disk_info)
    print(network_info)
    db_conn = create_remote_mysql_conn()
    try:
        with db_conn.cursor() as db_cursor:
            # 服务器基础信息表插入语句
            idc='bj10'
            insert_general_sql = """
                replace into hardware_general_info(idc,server_hostname,server_os,mem_total,mem_used,mem_available,l_cpu_size,p_cpu_size,boot_time)
                values('{0}','{1}','{2}','{3}','{4}','{5}',{6},{7},'{8}')
            """.format(idc, os_info['host_name'], os_info['linux_distribution'], os_info['mem_total'], os_info['mem_used'], os_info['mem_available'], os_info['l_cpu_count'],os_info['p_cpu_count'],os_info['boot_time'])
            print(insert_general_sql)
            # 插入最新查询的数据
            db_cursor.execute(insert_general_sql)

            # 磁盘分区详情表插入语句
            for item in disk_info:
                insert_partition_sql = """
                    replace into hardware_disk_detail(server_hostname,mount_point,part_path,part_total,part_used,part_free,
                    part_usedper) values ('{0}','{1}','{2}',{3},{4},{5},{6})
                """.format(host_name, item['part_mountpoint'], item['part_path'], item['part_total'],
                           item['part_used'], item['part_free'], item['part_usedper'])
                # 插入或更新数据
                db_cursor.execute(insert_partition_sql)
                #print(insert_partition_sql)

            # 网卡信息表插入语句
            for item in network_info:
                insert_network_sql = """
                    replace into hardware_net_detail(server_hostname,net_card_name,net_addr_ip,net_speed) values ('{0}', '{1}', '{2}', '{3}')
                """.format(host_name, item['net_card_name'], item['net_addr_ip'], item['net_speed'])
                db_cursor.execute(insert_network_sql)

        # 提交所有数据库变更
        db_conn.commit()
        logger.info("Agent execute success!")
    except db.OperationalError as e:
        logger.warning("SQL execute failed, Msg: ({0}, {1})".format(e.args[0], e.args[1]))
        # 若执行失败，则所有变更都进行回滚
        db_conn.rollback()
    finally:
        # 关闭游标和数据库连接
        db_cursor.close()
        db_conn.close()


def main():
    collect_all_info()


if __name__ == "__main__":
    main()

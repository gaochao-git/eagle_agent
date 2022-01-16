#!/usr/bin/python35
# -*- coding: utf-8 -*-
# @Author  : gaochao
"""
定期检查,看是否需要更新agent的配置信息
"""

import socket
import sys
import os
import pymysql
import time
import yaml
from psutil import pid_exists
import logging

PROJECT_PATH = '/tmp/eagle_agent/'
PID_FILE = PROJECT_PATH + 'pid/agent_daemon.pid'
DAEMON_FILE = PROJECT_PATH + 'bin/agent_daemon.py'

sys.path.append(PROJECT_PATH)

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
handler = logging.FileHandler(PROJECT_PATH + "log/check_config.log")
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
config_dict = dict()
host_name = socket.gethostname()

connection = pymysql.connect(host='10.88.132.153',
                             user='dba_agent',
                             password='qsgXzYMkTx8WBcRE',
                             db='eagle_agent',
                             charset='utf8mb4',
                             port=3306,
                             cursorclass=pymysql.cursors.DictCursor)


def read_config():
    global config_dict
    with open(os.path.join(os.path.dirname(__file__), PROJECT_PATH + 'config/agent_config.yml')) as f:
        config_dict = yaml.load(f)


def get_database_config():
    try:
        with connection.cursor() as cursor:
            sql = 'select  `command_name`,`param`,`interval` ,`package` FROM agent_info WHERE host_name=%s and `status`=%s'
            number = cursor.execute(sql, (host_name, 'enable'))
            if not number:
                return dict()
            command_in_database = cursor.fetchall()
            config_in_database = format_command(command_in_database)
    except Exception as e:
        logger.exception(e)
        return None
    return config_in_database


def git_pull_if_need():
    try:
        with connection.cursor() as cursor:
            sql = 'select  *  FROM agent_host_status WHERE host_name=%s and `project_status`=%s'
            number = cursor.execute(sql, (host_name, 'change'))
            if not number:
                return True
            import random
            random.seed(host_name)
            time.sleep(random.randint(1, 10))  # 随机休息秒,降低同时git pull的概率
            if os.system('cd ' + PROJECT_PATH + ' && git pull'):  # 执行失败时直接返回
                return False
            update_sql = 'update agent_host_status set `project_status`=%s where host_name=%s'
            cursor.execute(update_sql, ('done', host_name))
            connection.commit()
            restart_cron_process()
    except Exception as e:
        logger.exception(e)
        connection.rollback()
        return False
    return True


def format_command(command_in_database):
    formatted_config = dict()
    for agent in command_in_database:
        agent_config = dict()
        s = agent['param']
        if s == '':
            agent.pop('param')
        else:
            agent['param'] = eval(s)
        agent_config[agent['command_name']] = agent
        formatted_config.update(agent_config)
    return formatted_config


def write_config():
    global config_dict
    with open(os.path.join(os.path.dirname(__file__), PROJECT_PATH + 'config/agent_config.yml'), 'w') as f:
        yaml.dump(config_dict, f)


def check_config():
    global config_dict
    check_cron_process()
    read_config()
    config_in_database = get_database_config()
    if config_in_database == None:  # 证明发生了异常
        return
    config_dict.pop('version')
    # if not git_pull_if_need():
    #     return
    if config_in_database == config_dict:
        return
    config_dict = config_in_database
    config_dict['version'] = 1
    write_config()
    restart_cron_process()


def check_cron_process():
    if os.path.exists(PID_FILE):
        with open(PID_FILE) as f:
            try:
                pid = int(f.read())
                if pid_exists(pid):
                    return
                os.remove(PID_FILE)
            except Exception as e:
                logger.exception(e)
    os.system(DAEMON_FILE + ' start')


def restart_cron_process():
    try:
        os.system(DAEMON_FILE + ' stop')
        os.system(DAEMON_FILE + ' start')
    except Exception as e:
        logger.exception(e)


if __name__ == '__main__':
    check_config()

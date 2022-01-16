#!/usr/bin/python35
# -*- coding: utf-8 -*-
# @Author  : gaochao
# agent_daemon.py
import asyncio
import atexit
import logging
import logging.config
import socket

import pymysql as db
import os
import signal
import sys
from functools import partial
from importlib import import_module

import yaml

PROJECT_PATH = '/srv/eagle_agent/'
PID_FILE = PROJECT_PATH + 'pid/agent_daemon.pid'
sys.path.append(PROJECT_PATH)


HOSTNAME = socket.gethostname()

# 从项目配置文件获取eagle_agent依赖的后端数据库地址
with open(os.path.join(os.path.dirname(__file__), PROJECT_PATH + 'config/eagle_agent.yml')) as f:
    eagle_agent_config = yaml.load(f, Loader=yaml.FullLoader)
    agent_db_connect_info = eagle_agent_config['eagle_db_test']

# 初始化数据库连接    
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
db_conn_agent = conn_mysql_instance(agent_db_connect_info['mysql_ip'], agent_db_connect_info['mysql_port'], agent_db_connect_info['mysql_user'],agent_db_connect_info['mysql_pass'],agent_db_connect_info['mysql_db'])

# 获取日志配置文件
with open(os.path.join(os.path.dirname(__file__), PROJECT_PATH + 'config/logger.yml')) as f:
    try:
        logger_config = yaml.load(f, Loader=yaml.FullLoader)
    except Exception:
        logger_config = yaml.load(f)


logging.config.dictConfig(logger_config)
logger = logging.getLogger('agent_logger')


def daemonize(pidfile, *, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
    if os.path.exists(pidfile):
        raise RuntimeError('Already running')

    # First fork (detaches from parent)
    try:
        if os.fork() > 0:
            raise SystemExit(0)  # Parent exit
    except OSError as e:
        raise RuntimeError('fork #1 failed.')

    os.chdir('/')
    os.umask(0)
    os.setsid()
    # Second fork (relinquish session leadership)
    try:
        if os.fork() > 0:
            raise SystemExit(0)
    except OSError as e:
        raise RuntimeError('fork #2 failed.')

    # Flush I/O buffers
    sys.stdout.flush()
    sys.stderr.flush()

    # Replace file descriptors for stdin, stdout, and stderr
    with open(stdin, 'rb', 0) as f:
        os.dup2(f.fileno(), sys.stdin.fileno())
    with open(stdout, 'ab', 0) as f:
        os.dup2(f.fileno(), sys.stdout.fileno())
    with open(stderr, 'ab', 0) as f:
        os.dup2(f.fileno(), sys.stderr.fileno())

    # Write the PID file
    with open(pidfile, 'w') as f:
        print(os.getpid(), file=f)

    # Arrange to have the PID file removed on exit/signal
    atexit.register(lambda: os.remove(pidfile))

    # Signal handler for termination (required)
    def sigterm_handler(signo, frame):
        raise SystemExit(1)

    signal.signal(signal.SIGTERM, sigterm_handler)


# 更改agent运行时间
def agent_success_run(module):
   try:
       with db_conn_agent.cursor() as db_cursor:
           sql = " update agent_config_info set update_time=CURRENT_TIMESTAMP where host_name= '{hostname}' and command_name='{module}' and status = 'enable'".format(hostname=HOSTNAME, module=module)
           db_cursor.execute(sql)
           db_conn.commit()
   except Exception as e:
       db_conn_agent.rollback()
       logger.error(e)
   finally:
       db_cursor.close()
       db_conn_agent.close()


# agent运行类
class Agent:
    def __init__(self, module, module_name, interval, loop, executor, param_dict):
        self.module = module
        self.interval = interval
        self.module_name = module_name
        self.loop = loop
        self.executor = executor
        self.param_dict = param_dict
        asyncio.ensure_future(self.exectue())

    async def exectue(self):
        while True:
            try:
                await asyncio.sleep(self.interval)
                logger.info("%s 开始执行" % self.module_name)
                task = self.loop.run_in_executor(self.executor, partial(self.module.main, **self.param_dict))
                await task
                #agent_success_run(self.module_name)
                logger.info("%s 执行成功" % self.module_name)
            except Exception as e:
                logger.error(str(e))
                logger.exception(e)
                logger.error("%s 执行失败" % self.module_name)

# 初始化日志
def init_logger():
    import logging.config
    import yaml
    # import logging.handlers
    with open(os.path.join(os.path.dirname(__file__), PROJECT_PATH + 'config/logger.yml')) as f:
        try:
            logger_config = yaml.load(f, Loader=yaml.FullLoader)
        except Exception:
            logger_config = yaml.load(f)
    logging.config.dictConfig(logger_config)


def main():
    """这种方式到是使用了进程池的思想,但当任务比较多时,会不会导致后续任务不能及时执行?,或者说根据配置文件生成进程池内的进程数目?"""
    init_logger()
    logger.info("agent start run ")
    from concurrent.futures import ThreadPoolExecutor
    with open(os.path.join(os.path.dirname(__file__), PROJECT_PATH + 'config/agent_config.yml')) as f:
        config_dict = yaml.load(f)
    thread_num = len(config_dict) if len(config_dict) < 20 else 20
    executor = ThreadPoolExecutor(thread_num)
    loop = asyncio.get_event_loop()
    loop.set_default_executor(executor)
    agent_list = list()
    config_dict.pop('version')
    for module_name, module_info in config_dict.items():
        logger.info(module_info)
        module_name = module_info['package'] + '.' + module_name
        logger.info(module_name)
        module = import_module(module_name, __package__)
        logger.info(module)
        agent_list.append(
            Agent(module, module_info['command_name'], module_info['interval'], loop, executor,module_info.get('param', dict())))
    loop.run_forever()

# 启动后台进程
def daemonize_start():
    try:
        log_path = PROJECT_PATH + 'log/'
        if not os.path.isfile(log_path + 'daemon_success.log'):
            os.mknod(log_path + 'daemon_success.log')
        if not os.path.isfile(log_path + 'daemon_error.log'):
            os.mknod(log_path + 'daemon_error.log')
        daemonize(PID_FILE,
                  stdout=log_path + 'daemon_success.log',
                  stderr=log_path + 'daemon_error.log')
    except RuntimeError as e:
        print(e, file=sys.stderr)
        raise SystemExit(1)

# 停止后台进程
def daemonize_stop():
    if os.path.exists(PID_FILE):
        with open(PID_FILE) as f:
            os.kill(int(f.read()), signal.SIGTERM)
    else:
        print('Not running', file=sys.stderr)
        raise SystemExit(1)


if __name__ == '__main__':

    if len(sys.argv) != 2:
        print('Usage: {} [start|stop]'.format(sys.argv[0]), file=sys.stderr)
        raise SystemExit(1)

    if sys.argv[1] == 'start':
        daemonize_start()
        main()

    elif sys.argv[1] == 'stop':
        daemonize_stop()

    else:
        print('Unknown command {!r}'.format(sys.argv[1]), file=sys.stderr)
        raise SystemExit(1)

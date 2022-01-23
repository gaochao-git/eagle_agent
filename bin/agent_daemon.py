#!/usr/bin/python35
# -*- coding: utf-8 -*-
# @Author  : gaochao
# agent_daemon.py
import asyncio
import atexit
import logging
import logging.config
import socket
import psutil
import re
import os
import signal
import sys
from functools import partial
from importlib import import_module
import yaml

# 将执行文件所在目录的上层目录追加到包引用路径中,便于包引用
PROJECT_PATH = os.path.abspath(os.path.dirname(os.path.dirname(__file__))).rstrip('/')
sys.path.append(PROJECT_PATH)
from utils.db_helper import DbHelper
db_op_obj = DbHelper()

# 读取项目配置
with open(PROJECT_PATH + '/config/agent_config.yml') as f:
    agent_config = yaml.load(f, Loader=yaml.FullLoader)
    # 获取日志路径
    project_logdir = agent_config['logdir'].rstrip('/')
    # 获取pid路径
    project_piddir = agent_config['piddir'].rstrip('/')
    # 如果路径不存在则创建
    if not os.path.exists(project_logdir): os.makedirs(project_logdir)
    if not os.path.exists(project_piddir): os.makedirs(project_piddir)
    # 创建pid文件
    PID_FILE = project_piddir + '/agent_daemon.pid'

# 初始化日志
with open(PROJECT_PATH + '/config/logger.yml') as f:
    try:
        logger_config = yaml.load(f, Loader=yaml.FullLoader)
        logger_config['handlers']['info_handler']['filename'] = project_logdir + '/' + 'agent.log'
        logger_config['handlers']['error_handler']['filename'] = project_logdir + '/' + 'agent.log'
    except Exception as e:
        print(e)
logging.config.dictConfig(logger_config)
logger = logging.getLogger('agent_logger')


def daemonize(pidfile, *, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
    """
    启动后台守护进程
    遍历/proc/所有进程号，并判断cmdline，防止多个任务重复启动，判断时要忽略自己，直接通过pid文件判断太简单容易误判
    """
    pids = psutil.pids()
    for pid in pids:
        match_cmd = None
        try:
            p = psutil.Process(pid)
            process_cmdline_info_list = p.cmdline()
            match_cmd = process_cmdline_info_list[1]
        except IndexError:
            pass  # 不需要打印到日志,因为有些进程号是瞬间的,会误导
        except Exception as e:
            print(e)
        if match_cmd and re.findall('(agent_daemon)', match_cmd) and p.pid != os.getpid():
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

    # 程序退出时的回调函数
    atexit.register(lambda: os.remove(pidfile))

    # Signal handler for termination (required)
    def sigterm_handler(signo, frame):
        raise SystemExit(1)

    signal.signal(signal.SIGTERM, sigterm_handler)


# 更改agent运行时间
def agent_success_run(module):
    HOSTNAME = socket.gethostname()
    sql = """
        update agent_config_info set update_time=CURRENT_TIMESTAMP 
        where host_name= '{hostname}' and command_name='{module}' and status = 'enable'
    """.format(hostname=HOSTNAME, module=module)
    db_op_obj.dml(sql)


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
                # agent_success_run(self.module_name)
                logger.info("%s 执行完成" % self.module_name)
            except Exception as e:
                logger.error(str(e))
                logger.exception(e)
                logger.error("%s 执行失败" % self.module_name)


def main():
    """这种方式到是使用了进程池的思想,但当任务比较多时,会不会导致后续任务不能及时执行?,或者说根据配置文件生成进程池内的进程数目?"""
    logger.info("agent start run ")
    from concurrent.futures import ThreadPoolExecutor
    with open(PROJECT_PATH + '/config/task.yml') as f:
        config_dict = yaml.load(f, Loader=yaml.FullLoader)
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


def daemonize_start():
    """
    启动后台守护进程
    """
    try:
        daemon_log = project_logdir + '/daemon.log'
        if not os.path.isfile(daemon_log): os.mknod(daemon_log)
        daemonize(PID_FILE, stdout=daemon_log, stderr=daemon_log)
    except RuntimeError as e:
        print(e, file=sys.stderr)
        raise SystemExit(1)


def daemonize_stop():
    """
    停止后台守护进程
    1.不能读取到pid文件里面的pid就去kill,如果该pid进程异常挂掉,里面的进程号有可能时别的进程的,直接kill会把别的进程kill掉,出现大故障
    2.匹配不到pid文件,并不代表该进程已经停止,需要通过process判断
    """
    if os.path.exists(PID_FILE):
        try:
            with open(PID_FILE) as f:
                pid = int(f.read())
                p = psutil.Process(pid)
                process_cmdline_info_list = p.cmdline()
                match_cmd = process_cmdline_info_list[1]
                if re.findall('(agent_daemon)', match_cmd):
                    os.kill(pid, signal.SIGTERM)
                else:
                    print("pidfile中pid与匹配到的pid不是一个进程,退出操作", file=sys.stderr)
                    raise SystemExit(1)
        except psutil.NoSuchProcess:
            print("没有匹配到进程号(%d)" % pid, file=sys.stderr)
            raise SystemExit(1)
        except Exception as e:
            print(str(e), file=sys.stderr)
            raise SystemExit(1)
    else:
        pids = psutil.pids()
        for pid in pids:
            match_cmd = None
            match_type = None
            running_flag = False
            try:
                p = psutil.Process(pid)
                process_cmdline_info_list = p.cmdline()
                match_cmd = process_cmdline_info_list[1]
                match_type = process_cmdline_info_list[2]
            except IndexError:
                pass  # 不需要打印到日志,因为有些进程号是瞬间的,会误导
            except Exception as e:
                print(e)
            if match_cmd and re.findall('(agent_daemon)', match_cmd) and match_type == "start":
                running_flag = True
                break
        if running_flag:
            os.kill(pid, signal.SIGTERM)
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

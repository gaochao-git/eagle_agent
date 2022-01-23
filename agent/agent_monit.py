#!/usr/bin/env python
# -*- coding: utf-8 -*-
# gaochao
# 监控agent资源,防止agent占用服务器过多资源,给出预警或者自杀agent守护进程
import time
import os
import collections
import platform as pf
import psutil
import pymysql as db
import logging
logger = logging.getLogger('agent_logger')
from utils.db_helper import DbHelper
db_op_obj = DbHelper()


def get_mem_info():
    """
    获取内存信息,大小单位为M
    """
    mem = int(psutil.Process(os.getpid()).memory_info().rss / 1024/ 1024)
    mem_percent = psutil.Process(os.getpid()).memory_percent()
    logger.info("agent占用内存资源%d M,占总内存%{:.2f}" % (mem, mem_percent))


def get_cpu_info():
    """
    获取cpu占用率
    """
    cpu = psutil.Process(os.getpid()).cpu_percent(interval=1)
    logger.info("agent占用CPU资源%{:.2f}".format(cpu))


def get_disk_info():
    """
    获取磁盘信息,大小单位为M
    """
    pass


def get_network_info():
    """
    获取网络相关信息
    """
    pass


def main():
    get_mem_info()
    get_cpu_info()


if __name__ == "__main__":
    main()

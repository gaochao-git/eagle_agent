#!/usr/bin/env python
# -*- coding: utf-8 -*-
# gaochao
# 监控agent资源,防止agent占用服务器过多资源,给出预警或者自杀agent守护进程
import time
import os
import collections
import platform as pf
import psutil
import math
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
    logger.info("agent占用内存资源{}M,占总内存%{:.2f}".format(mem, mem_percent))


def get_cpu_info():
    """
    获取cpu占用率
    """
    l_cpu_count = psutil.cpu_count()
    cpu_percent = psutil.Process(os.getpid()).cpu_percent(interval=1)
    cpu_percent_total = int(math.ceil(cpu_percent)) / (l_cpu_count * 100) * 100
    logger.info("agent占用CPU资源%{:.2f},占用总CPU资源%{:.2f}".format(cpu_percent, cpu_percent_total))


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

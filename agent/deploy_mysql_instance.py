#!/bin/envs python
#-*- coding:utf-8 -*-

import pymysql
import socket
import os
import tarfile
import re
import logging.handlers
import sys
import hashlib
import json
from shutil import copyfile
from utils.db_helper import DbHelper
import logging
logger = logging.getLogger('agent_logger').getChild(__name__)



#定时上报已经安装的mysql端口,供页面展示可用端口
def report_mysql_port(host_ip):
    # 从正在运行端口获取
    running_port_list = []
    pids = [pid for pid in os.listdir('/proc') if pid.isdigit()]
    for pid in pids:
        try:
            process_cmd_info_list = open(os.path.join('/proc', pid, 'cmdline'), 'rb').read().split(b'\0')
            match_cmd = process_cmd_info_list[0].decode('utf-8')
            if re.findall('(mysqld)',match_cmd) and re.findall('(--port)',process_cmd_info_list[-2].decode('utf-8')):
                running_port = process_cmd_info_list[-2].decode('utf-8').split('=')[-1]
                if running_port not in running_port_list:
                    running_port_list.append(int(running_port))
        except Exception as e:
            print(e)   #不需要打印到日志,因为有些进程号是瞬间的,会误导
    for running_port in running_port_list:
        sql = "replace into deployed_mysql_port(host_ip,port) values('{}',{})".format(host_ip,running_port)
        DbHelper.dml(sql)
    # 从安装目录获取
    port_list = [3306,3307,3308,3309,3310,3311,3312,3313,3314,3315]
    for check_port in port_list:
        if os.path.exists('/data/{}'.format(check_port)) or os.path.exists('/data/mysql/multi/{}'.format(check_port)):
            sql = "replace into deployed_mysql_port(host_ip,port) values('{}',{})".format(host_ip,check_port)
            DbHelper.dml(sql)

# 获取主机ip
def get_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip

def get_disk_free_capacity():
    return 100

# 写日志
def write_log(deploy_task_info,log):
    submit_uuid = deploy_task_info[0]["submit_uuid"]
    host_ip = deploy_task_info[0]["host_ip"]
    port = deploy_task_info[0]["port"]
    insert_sql = "insert into deploy_mysql_instance_log(submit_uuid,host_ip,port,deploy_log) values('{}','{}','{}','{}')".format(submit_uuid,host_ip,port,log)
    DbHelper.dml(insert_sql)

# 更改状表状态
def update_status(deploy_task_info,deploy_status):
    submit_uuid = deploy_task_info[0]["submit_uuid"]
    update_sql = "update deploy_mysql_instance set deploy_status={} where submit_uuid='{}'".format(deploy_status,submit_uuid)
    if deploy_status == 3:
        DbHelper.dml(update_sql)
    elif deploy_status == 2:
        DbHelper.dml(update_sql)
    elif deploy_status == 1:
        DbHelper.dml(update_sql)

# 解压文件
def tar_zxf(deploy_task_info,fname,extract_target_dir):
    print('开始解压安装包')
    write_log(deploy_task_info,"开始解压安装包")
    package_path_name = extract_target_dir + '/' + fname
    copyfile("/srv/percona-toolkit-3.2.0_x86_64.tar.gz","/srv/nucc_mysql.tar.gz")
    try:
        t = tarfile.open(package_path_name)
        t.extractall(path = extract_target_dir)
        print("解压安装包成功")
        write_log(deploy_task_info,"解压安装包成功")
        return True
    except Exception as e:
        print(e)
        print("解压压缩包失败,退出此次任务")
        write_log(deploy_task_info,"解压压缩包失败,退出此次任务")
        deploy_status = 3
        update_status(deploy_task_info,deploy_status)
        raise Exception("解压压缩包失败,退出此次任务")
# 获取md5
def get_md5(filename):
    myhash = hashlib.md5()
    f = open(filename,'rb')
    while True:
        b = f.read(8096)
        if not b :
            break
        myhash.update(b)
    f.close()
    return myhash.hexdigest()

# 替换指定配置文件参数
def modify_arg_value(deploy_task_info,file,new_arg_k_v_line):
    try:
        new_arg_k = new_arg_k_v_line.split("=")[0].strip()
        new_arg_v = new_arg_k_v_line.split("=")[1].strip()
        file_data = ""
         #将所有行读出并替换指定行内容
        with open(file, "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith(new_arg_k):
                    old_arg_k = line.split("=")[0].strip()
                    old_arg_v = line.split("=")[1].strip()
                    if old_arg_k == new_arg_k and old_arg_v != new_arg_v:
                        log = "{}参数替换vaule:{}-->{}".format(old_arg_k,old_arg_v,new_arg_v)
                        print(log)
                        write_log(deploy_task_info,log)
                        line = line.replace(old_arg_v,new_arg_v)
                file_data += line
        # 重新将所有行写入
        with open(file,"w",encoding="utf-8") as f:
            f.write(file_data)
    except Exception as e:
        print(e)
        write_log(deploy_task_info,"修改配置文件出现错误")
        raise Exception("修改配置文件出现错误")

# 判断是否需要更改配置文件
def modify_config(deploy_task_info):
    deploy_port = deploy_task_info[0]['port']
    deploy_other_param_str = deploy_task_info[0]['deploy_other_param']
    deploy_other_param_json = json.loads(deploy_other_param_str)
    #my_conf_file = "/data/mysql/multi/{}/etc/my.cnf".format(deploy_port)
    my_conf_file = "/tmp/my.cnf"
    if len(deploy_other_param_json) > 1:
        print("开始修改自定义配置文件参数")
        write_log(deploy_task_info,"开始修改自定义配置文件参数")
        for k in deploy_other_param_json:
            line = k + "=" + deploy_other_param_json[k]
            modify_arg_value(deploy_task_info,my_conf_file,line)
        print("修改自定义配置文件参数完成")
        write_log(deploy_task_info,"修改自定义配置文件参数完成")
        return True
    else:
        write_log(deploy_task_info,"使用默认配置文件")
        print("使用默认配置文件")
        return True
# 开始部署
def deploy(task_content):
    deploy_task_info = task_content["deploy_task_info"]
    deploy_port = deploy_task_info[0]['port']
    deploy_archit = deploy_task_info[0]['deploy_archit']
    deploy_env = deploy_task_info[0]['deploy_env']
    print("开始调用初始化脚本进行初始化.....")
    write_log(deploy_task_info,"开始调用初始化脚本进行初始化.....")
    print("===================================")
    write_log(deploy_task_info,"===================================")
    #deploy_cmd = "/srv/mysql/scripts/mysql_multi.ini --port={} --version='{}' --env='{}'".format(deploy_port,deploy_archit,deploy_env)
    deploy_cmd = "/tmp/1.sh --port={} --version='{}' --env='{}'".format(deploy_port,deploy_archit,deploy_env)
    out_info = os.popen(deploy_cmd)
    logs = out_info.readlines()
    log_again =[]
    # 脚本日志写入日志表
    for log in logs:
        log_again.append(log)
        log = log.strip('\n')
        write_log(deploy_task_info,log) 
    if 'finish\n' in log_again:
        print("初始化完成")
        write_log(deploy_task_info,"初始化完成")
        print("检查是否需要更改配置文件默认参数")
        if modify_config(deploy_task_info):
            start_mysql(task_content)
    else:
        print("初始化失败")
        deploy_status = 3
        update_status(deploy_task_info,deploy_status)
def start_mysql(task_content):
    deploy_task_info = task_content["deploy_task_info"]
    deploy_port = deploy_task_info[0]['port']
    start_cmd = "/srv/mysql/scripts/mysql_multi.server -P {} start".format(deploy_port)
    print("开始启动mysql.....")
    write_log(deploy_task_info,"开始启动mysql.....")
    if 1==1:
        print("启动mysql成功")
        write_log(deploy_task_info,"启动mysql成功")
        deploy_status = 2
        update_status(deploy_task_info,deploy_status)
    else:
        print("启动mysql失败")
        deploy_status = 3
        update_status(deploy_task_info,deploy_status)
        write_log(deploy_task_info,"启动mysql失败")
# 部署前准备
def pre_deploy(task_content):
    extract_target_dir = "/srv"
    deploy_task_info = task_content["deploy_task_info"]
    package_info = task_content["package_info"]
    dowload_base_url = package_info[0]["pacakage_url"]
    fname = package_info[0]["package_name"]
    package_md5 = package_info[0]["package_md5"]
    package_file_path = extract_target_dir + '/' + fname
    write_log(deploy_task_info,"检查安装脚本或者安装包是否存在")
    if os.path.exists('/srv/mysql'):
        print("安装脚本存在,开始初始化....")
        write_log(deploy_task_info,"安装脚本存在,开始初始化....")
        deploy(task_content)
    elif os.path.exists(package_file_path):
        md5_current_file = get_md5(package_file_path) 
        if md5_current_file == package_md5:
            if tar_zxf(deploy_task_info,fname,extract_target_dir):
                deploy(task_content)
        else:
            os.remove(package_file_path)
            if download_package(deploy_task_info,dowload_base_url,fname,extract_target_dir):
                if tar_zxf(deploy_task_info,fname,extract_target_dir):
                    deploy(task_content)
    else:
        if download_package(deploy_task_info,dowload_base_url,fname,extract_target_dir):
            if tar_zxf(deploy_task_info,fname,extract_target_dir):
                deploy(task_content)
# 下载安装包
def download_package(deploy_task_info,dowload_base_url,fname,extract_target_dir):
    print("安装脚本及安装包均不存在,开始下载")
    write_log(deploy_task_info,"安装脚本及安装包均不存在,开始下载")
    #download_url = task_content["package_info"][0]["pacakage_url"] + task_content["package_info"][0]["package_name"]
    download_url = "https://dev.mysql.com/get/mysql80-community-release-el8-1.noarch.rpm"
    #download_url = dowload_base_url + fname
    download_cmd = "wget -P {}  -q -r -nH -l inf --limit-rate=10M --connect-timeout=5 --read-timeout=600 {}".format(extract_target_dir,download_url)
    status = os.system(download_cmd)
    if status == 0:
        print("下载安装包成功")
        write_log(deploy_task_info,"下载安装包成功")
        return True
    else:
        print("下载压缩包失败")
        write_log(deploy_task_info,"下载压缩包失败,退出此次任务")
        raise Exception("载压缩包失败,退出此次任务")
# 获取本机任务
def get_task_info(host_ip):
    # 开始获取任务
    deploy_info_sql = "select submit_uuid,host_ip,port,deploy_status,deploy_archit,deploy_env,deploy_other_param from deploy_mysql_instance where host_ip='{}' and deploy_status=0 and timestampdiff(second,ctime,now())<86400 limit 1" .format(host_ip)
    ret = DbHelper.find_all(deploy_info_sql)
    if ret['status'] != "ok": return False
    elif len(ret['data']) == 0: return False
    else:
        deploy_task_info = ret['data']
        log = "获取到部署任务"
        write_log(deploy_task_info,log)
        package_info_sql = "select pacakage_url,package_name,package_md5 from deploy_package_info where package_name='nucc_mysql.tar.gz'"
        ret = DbHelper.find_all(package_info_sql)
        if ret['status'] == "ok":
            if len(ret['data']) <=0: raise "没有获取到部安装包"
            package_info = ret['data']
            return {"task":"yes","deploy_task_info":deploy_task_info,'package_info':package_info}

def main():
    host_ip = get_ip()
    report_mysql_port(host_ip)
    task_content = get_task_info(host_ip)
    if task_content:
        deploy_task_info = task_content['deploy_task_info']
        deploy_status = 1
        update_status(deploy_task_info,deploy_status)
        pre_deploy(task_content)
if __name__ == '__main__':
    main()

#-*- coding: utf-8 -*-

import logging
import re
import subprocess
import traceback
import time
import datetime


# 自定义的日志输出
def log(msg, level = logging.DEBUG):
    logging.log(level, msg)
    print('%s [%s], msg:%s' % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), level, msg))

    if level == logging.WARNING or level == logging.ERROR:
        for line in traceback.format_stack():
            print(line.strip())

        for line in traceback.format_stack():
            logging.log(level, line.strip())


def kill_ports(ports):
    for port in ports:
        log('kill %s start' % port)
        popen = subprocess.Popen('lsof -i:%s' % port, shell = True, stdout = subprocess.PIPE)
        (data, err) = popen.communicate()
        log('data:\n%s  \nerr:\n%s' % (data, err))

        pattern = re.compile(r'\b\d+\b', re.S)
        pids = re.findall(pattern, data)

        log('pids:%s' % str(pids))

        for pid in pids:
            if pid != '' and pid != None:
                try:
                    log('pid:%s' % pid)
                    popen = subprocess.Popen('kill -9 %s' % pid, shell = True, stdout = subprocess.PIPE)
                    (data, err) = popen.communicate()
                    log('data:\n%s  \nerr:\n%s' % (data, err))
                except Exception, e:
                    log('kill_ports exception:%s' % e)

        log('kill %s finish' % port)

    time.sleep(1)


def get_create_table_command(table_name):
    command = (
        "CREATE TABLE IF NOT EXISTS {} ("
        "`id` INT(8) NOT NULL AUTO_INCREMENT UNIQUE ,"
        "`title` TEXT NOT NULL,"
        "`average` FLOAT NOT NULL,"
        "`rating_people` INT(7) DEFAULT NULL,"
        "`rating_five` CHAR(5) DEFAULT NULL,"
        "`rating_four` CHAR(5) DEFAULT NULL ,"
        "`info_director` CHAR(20) DEFAULT NULL,"
        "`info_screenwriter` CHAR(20) DEFAULT NULL,"
        "`info_starred` CHAR(20) DEFAULT NULL,"
        "`info_type` CHAR(20) DEFAULT NULL,"
        "`info_region` CHAR(20) DEFAULT NULL,"
        "`info_language` CHAR(20) DEFAULT NULL,"
        "`info_release_date` CHAR(40) DEFAULT NULL,"
        "`info_runtime` CHAR(20) DEFAULT NULL,"
        "`info_other_name` TEXT DEFAULT NULL,"
        "`info_describe` TEXT DEFAULT NULL,"
        "`url` TEXT NOT NULL,"
        "`save_time` TIMESTAMP NOT NULL,"
        "PRIMARY KEY(id)"
        ") ENGINE=InnoDB".format(table_name))
    return command


def get_insert_data_command(table_name):
    command = ("INSERT IGNORE INTO {} "
               "(id, title, average, rating_people, rating_five, rating_four, info_director, info_screenwriter, "
               "info_starred, info_type, info_region, info_language, info_release_date, info_runtime, "
               "info_other_name, info_describe, url, save_time)"
               "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(table_name))

    return command

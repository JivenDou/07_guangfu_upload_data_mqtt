"""
@File  : log_config.py
@Author: lee
@Date  : 2022/7/13/0013 11:08:55
@Desc  :
"""
import logging
import sys

LOGGING_CONFIG = dict(
    version=1,
    disable_existing_loggers=False,
    loggers={
        # 新曾自定义日志，用于数据采集程序
        "server_mqtt_log": {
            "level": "DEBUG",
            "handlers": ["console", "server_mqtt_log"],
            "propagate": True,
            "qualname": "server_mqtt_log.debug",
        },
        "mysql_database_log": {
            "level": "DEBUG",
            "handlers": ["console", "mysql_database_log"],
            "propagate": True,
            "qualname": "mysql_database_log.debug",
        }
    },
    handlers={
        # 数据采集程序控制台输出handler
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "generic",
            "stream": sys.stdout,
        },
        "server_mqtt_log": {
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': 'log/server_mqtt_log/server_mqtt_log.log',
            'maxBytes': 10 * 1024 * 1024,
            'delay': True,
            "formatter": "generic",
            "backupCount": 20,
            "encoding": "utf-8"
        },
        "mysql_database_log": {
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': 'log/mysql_database_log/mysql_database_log.log',
            'maxBytes': 10 * 1024 * 1024,
            'delay': True,
            "formatter": "generic",
            "backupCount": 20,
            "encoding": "utf-8"
        }
    },
    formatters={
        # 自定义文件格式化器
        "generic": {
            "format": "%(asctime)s [%(filename)s:%(lineno)d] [%(levelname)s] %(message)s",
            "datefmt": "[%Y-%m-%d %H:%M:%S]",
            "class": "logging.Formatter",
        }
    },
)
server_mqtt_log = logging.getLogger("server_mqtt_log")
mysql_database_log = logging.getLogger("mysql_database_log")

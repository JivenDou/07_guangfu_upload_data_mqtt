#!/usr/bin/env python
import time
import configparser
import json
import hashlib
import paho.mqtt.client as mqtt
import logging.config
import os

from tools.mysqlDataBase import MysqldbOperational
from Crypto.Cipher import AES
from tools.AES_crypt import AESCrypt
from tools import LogOut
from tools.logging_config import LOGGING_CONFIG
from tools.logging_config import server_mqtt_log as logger
from tools.logging_config import mysql_database_log as sql_logger

# logging config
logging.config.dictConfig(LOGGING_CONFIG)
handlers = LOGGING_CONFIG['handlers']
for handler in handlers:
    item = handlers[handler]
    if 'filename' in item:
        filename = item['filename']
        dirname = os.path.dirname(filename)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
# --------------------------


def getCodeAndPoint(addrData):
    str = "."
    code = addrData[:addrData.index(str)]
    point = addrData[addrData.index(str) + 1:]
    return code, point


def getDeviceConnectionStatus(data_time_stamp):
    now_time_stamp = time.time()
    if data_time_stamp < now_time_stamp - 6000:
        return 1
    else:
        return 0


def dateAndTimeToTimestamp(date_time):
    time_array = time.strptime(date_time, "%Y-%m-%d %H:%M:%S")
    time_stamp = int(time.mktime(time_array))
    return time_stamp


def setSendStatusIsSucceed(dataInfo):
    """
    设置成功上传数据的设备的is_send置1
    :param dataInfo: [{'id': 1, 'tableName': 'CR1000X_1', 'is_avg': 1}]
    """
    if dataInfo is not None:
        for i in range(len(dataInfo)):
            t_id = dataInfo[i]['id']
            is_avg = dataInfo[i]['is_avg']
            if is_avg:
                table_name = "copy_table_" + str(dataInfo[i]['tableName'])
            else:
                table_name = "table_" + str(dataInfo[i]['tableName'])
            my.set_send_status(table_name, t_id, 1)


def getMqttDataFromMysql(list_devices, appId=None, token=None):
    """
    获取打包数据
    :param list_devices:[{'device_name': 'GI410_A', 'is_avg': 0},..., {'device_name': 'SVM30_5', 'is_avg': 1}]
    :param appId:
    :param token:
    :return: param（打包数据）, dataInfo（有效数据设备）
    """
    dataList = []
    data = {}
    data_from_mysql_info_list = []
    for device in list_devices:
        device_name = device['device_name']
        is_avg = device['is_avg']
        list_points = my.get_mqtt_point(device_name)
        if is_avg:
            table_name = 'copy_table_' + str(list_points[0]['deviceName'])
        else:
            table_name = 'table_' + str(list_points[0]['deviceName'])
        data_from_mysql = my.get_newest_data(table_name)
        # logger.info(list_points)
        # logger.info(data_from_mysql)
        # 判断最新数据超过5分钟为设备离线
        if len(data_from_mysql) > 0:
            date_time = str(data_from_mysql['times'])
            date_time = dateAndTimeToTimestamp(date_time)
            STS_DI = getDeviceConnectionStatus(date_time)
            # logger.info("STS_DI = ", STS_DI)
        else:
            STS_DI = 1
        dataDist = {}
        data_from_mysql_info_dict = {}

        # 设备在线时
        if STS_DI == 0:
            dataDist['DQ_DI'] = '0'
            dataDist['STS_DI'] = '0'
            dataDist['code'] = list_points[0]['deviceName']
            dataDist['ts'] = int(round(time.time() * 1000))
            dataDist['times'] = str(data_from_mysql['times'])
            data_from_mysql_info_dict['id'] = data_from_mysql['id']
            data_from_mysql_info_dict['tableName'] = list_points[0]['deviceName']
            data_from_mysql_info_dict['is_avg'] = is_avg
            data_from_mysql_info_list.append(data_from_mysql_info_dict)
            for i in range(len(list_points)):
                # [{'deviceName': 'GI410_A', 'serialNumber': 1, 'storageType': 'float', 'mqttCode': None,'lowLimit': -90.0, 'upLimit': 90.0},
                #  {'deviceName': 'GI410_A', 'serialNumber': 2, 'storageType': 'float', 'mqttCode': None,'lowLimit': -180.0, 'upLimit': 180.0}]
                # code, point = getCodeAndPoint(list_points[i]['mqttCode'])
                columnName = "c" + str(list_points[i]['serialNumber'])
                dataDist[columnName] = str(data_from_mysql[columnName])

                if list_points[i]['lowLimit'] is not None and list_points[i]['upLimit'] is not None:
                    if dataDist[columnName] == "None" or float(dataDist[columnName]) < list_points[i]['lowLimit'] or \
                            float(dataDist[columnName]) > list_points[i]['upLimit']:
                        # dataDist['DQ_DI'] = '1'  # 功能优化
                        dataDist.pop(columnName)  # 功能优化，过滤坏数据

            dataList.append(dataDist)
            # logger.info(dataDist)
        # 设备离线时
        elif STS_DI == 1:
            # code, point = getCodeAndPoint(list_points[0]['mqttCode'])
            dataDist['DQ_DI'] = '1'
            dataDist['STS_DI'] = '1'
            dataDist['code'] = list_points[0]['deviceName']
            dataDist['ts'] = int(round(time.time() * 1000))
            dataList.append(dataDist)
            # logger.info(dataDist)

    data['data'] = dataList
    # data['appId'] = appId
    # data['token'] = token
    param = json.dumps(data)
    # param = ''
    return param, data_from_mysql_info_list


def on_connect(client, userdata, flags, rc):
    """建立连接时"""
    if rc == 0:
        logger.info("Connected to MQTT Broker!")
    else:
        logger.error(f"Failed to connect, return code {rc}")


def on_message(client, userdata, msg):
    logger.info("主题:" + msg.topic + " 消息:" + str(msg.payload.decode('utf-8')))


def on_subscribe(client, userdata, mid, granted_qos):
    logger.info("On Subscribed: qos = %d" % granted_qos)


def on_disconnect(client, userdata, rc):
    if rc != 0:
        logger.info("Unexpected disconnection %s" % rc)


if __name__ == "__main__":
    _logger = LogOut.Log('iotServerMQTT')
    # 创建读取配置文件对象
    config = configparser.ConfigParser()
    config.read("config.ini", encoding="utf-8")

    # 获取通用配置项
    section = "General"  # 读取的部section标签
    mysql_host = config.get(section, 'mysqlHost')
    mysql_username = config.get(section, 'mysqlUsername')
    mysql_password = config.get(section, 'mysqlPassword')
    mysql_port = config.getint(section, 'mysqlPort')
    # token = config.get(section, 'token')
    # appId = config.get(section, 'appId')

    # 获取特有配置项
    section = 'iotServerMQTT'  # 读取的部section标签
    mysql_database = config.get(section, 'mysqlDatabase')
    HOST = config.get(section, 'mqttHost')
    PORT = config.getint(section, 'mqttPort')
    client_id = config.get(section, 'mqttClientId')
    key = config.get(section, 'mqttKey')  # keypassword
    username = config.get(section, 'mqttUsername')
    password = config.get(section, 'mqttPassword')
    topic = config.get(section, 'mqttTopic')
    frequency = config.getint(section, 'mqttFrequency')

    # 连接数据库
    my = MysqldbOperational(host=mysql_host,
                            username=mysql_username,
                            password=mysql_password,
                            port=mysql_port,
                            database=mysql_database,
                            logger=sql_logger)

    # 获取Aes加密配置项
    section = 'Aes'  # 读取的部section标签
    aes_key = config.get(section, 'aesKey')
    aes_cryptor = AESCrypt(aes_key, AES.MODE_ECB)  # ECB模式

    # mqtt客户端创建
    client = mqtt.Client(client_id)
    # client.tls_set(ca_certs='ca.crt', certfile=None, keyfile=None, cert_reqs=ssl.CERT_NONE,
    #                tls_version=ssl.PROTOCOL_TLSv1, ciphers=None)
    # client.tls_set_context(context=None)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_subscribe = on_subscribe
    client.on_disconnect = on_disconnect
    client.connect(HOST, PORT, 10)
    client.loop_start()

    # 获取点表中的设备列表
    list_devices = my.get_mqtt_devices()
    post_time = 0
    while True:
        time.sleep(0.1)
        if post_time < time.time() - frequency:
            # logger.info("------------------------------")
            print("------------------------------")
            try:
                post_time = time.time()
                # logger.info(list_devices)
                param, dataInfo = getMqttDataFromMysql(list_devices)
                print(param)
                aes128string = aes_cryptor.aesencrypt(json.dumps(param))
                is_send, mid = client.publish(topic, payload=aes128string.encode(), qos=1)
                # logger.info(datetime.now(), "is_send = ", is_send, "mid = ", mid)
                if is_send == 0:
                    setSendStatusIsSucceed(dataInfo)
            except Exception as e:
                logger.error("exception:", e)
    client.loop_stop()

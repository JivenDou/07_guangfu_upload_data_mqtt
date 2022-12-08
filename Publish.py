import paho.mqtt.client as mqtt
import logging.config
import os
import time
import json

from tools.hard_Disk_storage import HardDiskStorage
from Crypto.Cipher import AES
from tools.AES_crypt import AESCrypt
from tools.logging_config import LOGGING_CONFIG
# from tools.logging_config import publish_log as logger

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


def on_connect(client, userdata, flags, rc):
    """建立连接时"""
    if rc == 0:
        logger.info("Connected to MQTT Broker!")
    else:
        logger.error(f"Failed to connect, return code {rc}")


class MqttPubClient:
    def __init__(self, broker, client_id='', port=1883, timeout=60):
        self.__client = None
        self.__broker = broker
        self.__port = port
        self.__timeout = timeout
        self.__client_id = client_id

    def connect_mqtt(self, username, password=None):
        """建立连接"""
        try:
            self.__client = mqtt.Client(self.__client_id)
            self.__client.on_connect = on_connect
            self.__client.username_pw_set(username, password)
            self.__client.connect(host=self.__broker, port=self.__port, keepalive=self.__timeout)
            logger.info("connect success!")
            self.__client.loop_start()
        except Exception as e:
            logger.error(f"[MqttPubClient][connect_mqtt]connect error:{repr(e)}")

    def on_publish(self, topic, msg, qos):
        """发布消息"""
        while True:
            try:
                result = self.__client.publish(topic, msg, qos)
                status = result[0]
                if status == 0:
                    logger.info(f"Send `{msg}` to `{topic}`")
                    time.sleep(1)
                else:
                    logger.error(f"Failed to send message to topic {topic}")
                    time.sleep(5)
            except Exception as e:
                logger.error("[MqttPubClient][publish]error:", repr(e))
                time.sleep(5)


if __name__ == "__main__":
    p_client = MqttPubClient(broker='192.168.1.33')    # 创建发布者对象
    p_client.connect_mqtt(username="sencott", password="123456")  # 连接服务器
    # 获取数据
    # db = HardDiskStorage(user="root", passwd="zzZZ4144670..", db="shucai_huiyi", ip="127.0.0.1")
    datas = {'DQ_DI': 78.8354, 'code': '007001005BJB', 'ts': 1628114400000}
    # aes数据加密
    aes_passwd = "123456781234567"
    aes_cryptor = AESCrypt(aes_passwd, AES.MODE_ECB)  # ECB模式
    print(json.dumps(datas))
    aes128string = aes_cryptor.aesencrypt(json.dumps(datas))
    # 发布数据
    # p_client.on_publish(topic="test/1", msg=aes128string, qos=1)



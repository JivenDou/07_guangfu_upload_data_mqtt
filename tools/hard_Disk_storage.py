import pymysql
import traceback
import time


class HardDiskStorage:
    """操作数据库类"""
    def __init__(self, user, passwd, db, ip, port=3306, charset='utf8'):
        self.host = ip
        self.user = user
        self.passwd = passwd
        self.db = db
        self.port = port
        self.charset = charset
        self.conn = None
        self.cursor = None
        if not self._conn():
            self._reConn()

    def _conn(self):
        try:
            self.conn = pymysql.connect(host=self.host, user=self.user, password=self.passwd, database=self.db, port=self.port, autocommit=True)
            return True
        except Exception as e:
            print(f'failed to connect to {self.host}:{self.port}:{self.db} by [{self.user}:{self.passwd}]:{e}')
            return False

    def _reConn(self, num=28800, stime=3):  # 重试连接总次数为1天,这里根据实际情况自己设置,如果服务器宕机1天都没发现就......
        _number = 0
        _status = True
        while _status and _number <= num:
            try:
                self.conn.ping()  # cping 校验连接是否异常
                _status = False
            except Exception as e:
                print(e)
                if self._conn():  # 重新连接,成功退出
                    _status = False
                    break
                _number += 1
                time.sleep(stime)  # 连接不成功,休眠3秒钟,继续循环，知道成功或重试次数结束

    def close(self):
        self.conn.close()

    def execute_sql(self, sql):
        try:
            self._reConn()
            self.cursor = self.conn.cursor(cursor=pymysql.cursors.DictCursor)
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            self.cursor.close()
            return results
        except Exception as e:
            print(repr(e))
            print(traceback.format_exc())
            return None

import re
import pymysql
import time

"""
connect连接对象的方法：

close()  --关闭的方法

commit()   --如果支持事务则提交挂起的事务

rollback()  --回滚挂起的事务

cursor()  --返回连接的游标对象
游标对象的方法：

callproc(name,[params]) --用来执行存储过程，接收的参数为存储过程的名字和参数列表，返回受影响的行数

close()  --关闭游标

execute(sql,[params])--执行sql语句，可以使用参数，(使用参数时，sql语句中用%s进行站位注值),返回受影响的行数

executemany(sql,params)--执行单挑sql语句，但是重复执行参数列表里的参数，返回受影响的行数

fetchone()  --返回结果的下一行

fetchall()  --返回结果的 所有行

fetchmany(size)--返回size条记录，如果size大于返回结果行的数量，则会返回cursor.arraysize条记录

nextset()  --条至下一行

setinputsizes(size)--定义cursor

游标对象的属性：

description--结果列的描述,只读

rowcount  --结果中的行数，只读

arraysize  --fetchmany返回的行数，默认为1

"""


class MysqldbOperational(object):
    """
    操作mysql数据库，基本方法
    """

    # logger = LogOut.Log("mysql")

    def __init__(self, host="localhost", username="root", password="", port=3306, database="", logger=None,
                 charset='utf-8'):
        self.host = host
        self.username = username
        self.password = password
        self.database = database
        self.port = port
        self.charset = charset
        self.logger = logger
        self.con = None
        self.cur = None
        self._conn()

    def _conn(self):
        try:
            self.con = pymysql.connect(host=self.host, user=self.username, passwd=self.password, port=self.port,
                                       db=self.database, autocommit=True)
            # 所有的查询，都在连接 con 的一个模块 cursor 上面运行的
            self.cur = self.con.cursor()
        except Exception as e:
            # print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + "[ERROR] {0}\n".format(
            #     traceback.format_exc(limit=1)))
            self.logger.error('in function _conn.' + repr(e))

    def close(self):
        """
        关闭数据库连接
        """
        try:
            self.con.close()
            self.logger.info("Database connection close.")
        except Exception as e:
            # print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + "[ERROR] {0}\n".format(
            #     traceback.format_exc(limit=1)))
            self.logger.error('in function close.' + repr(e))

    def _reConn(self, num=28800, stime=3):  # 重试连接总次数为1天,这里根据实际情况自己设置,如果服务器宕机1天都没发现就......
        _number = 0
        _status = True
        while _status and _number <= num:
            try:
                self.con.ping()  # cping 校验连接是否异常
                _status = False
            except Exception as e:
                # print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + "[ERROR] {0}\n".format(
                #     traceback.format_exc(limit=1)))
                self.logger.error('in function _reConn.' + repr(e))
                if self._conn():  # 重新连接,成功退出
                    _status = False
                    break
                _number += 1
                time.sleep(stime)  # 连接不成功,休眠3秒钟,继续循环，知道成功或重试次数结束

    # 抽帧相关函数
    def getOneData(self):
        # 取得上个查询的结果，是单个结果
        data = self.cur.fetchone()
        return data

    def creatTable(self, tablename, attrdict, constraint):
        """创建数据库表
            args：
                tablename  ：表名字
                attrdict   ：属性键值对,{'book_name':'varchar(200) NOT NULL'...}DEFAULT NULL NOT NULL
                constraint ：主外键约束,PRIMARY KEY(`id`)  自动增量
        """
        self._reConn()
        if self.isExistTable(tablename):
            return
        sql = ''
        sql_mid = '`id` bigint(11) NOT NULL AUTO_INCREMENT,'
        for attr, value in attrdict.items():
            sql_mid = sql_mid + '`' + attr + '`' + ' ' + value + ','
        sql = sql + 'CREATE TABLE IF NOT EXISTS %s (' % tablename
        sql = sql + sql_mid
        sql = sql + constraint
        sql = sql + ') ENGINE=InnoDB DEFAULT CHARSET=utf8'
        print('creatTable:' + sql)
        self.executeCommit(sql)

    def executeSql(self, sql=''):
        """执行sql语句，针对读操作返回结果集

            args：
                sql  ：sql语句
        """
        try:
            self.cur.execute(sql)
            records = self.cur.fetchall()
            return records
        except pymysql.Error as e:
            # print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + "[ERROR] {0}\n".format(
            #     traceback.format_exc(limit=1)))
            self.logger.error('in function executeSql.' + repr(e))

    def executeCommit(self, sql=''):
        """执行数据库sql语句，针对更新,删除,事务等操作失败时回滚

        """
        try:
            self.cur.execute(sql)
            self.con.commit()
        except pymysql.Error as e:
            self.con.rollback()
            # print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + "[ERROR] {0}\n".format(
            #     traceback.format_exc(limit=1)))
            self.logger.error('in function executeCommit.' + repr(e))

            return False

    def insert(self, tablename, params):
        """插入数据库
            args：
                tablename  ：表名字
                key        ：属性键
                value      ：属性值
        """
        self._reConn()
        key = []
        value = []
        for tmpkey, tmpvalue in params.items():
            key.append(tmpkey)
            if isinstance(tmpvalue, str):
                value.append("\'" + tmpvalue + "\'")
            else:
                value.append(tmpvalue)
        attrs_sql = '(' + ','.join(key) + ')'
        values_sql = ' values(' + ','.join(value) + ')'
        sql = 'insert into %s' % tablename
        sql = sql + attrs_sql + values_sql
        print('_insert:' + sql)
        self.executeCommit(sql)

    def select(self, tablename, cond_dict='', order='', fields='*'):
        """查询数据
            args：
                tablename  ：表名字
                cond_dict  ：查询条件
                order      ：排序条件

            example：
                print mydb.select(table)
                print mydb.select(table, fields=["name"])
                print mydb.select(table, fields=["name", "age"])
                print mydb.select(table, fields=["age", "name"])
        """
        self._reConn()
        consql = ' '
        if cond_dict != '':
            for k, v in cond_dict.items():
                consql = consql + "`" + tablename + "`" + '.' + "`" + k + "`" + '=' + "'" + v + "'" + ' and'
        consql = consql + ' 1=1 '
        if fields == "*":
            sql = 'select * from %s where' % tablename
        else:
            if isinstance(fields, list):
                fields = ",".join(fields)
                sql = 'select %s from %s where' % (fields, tablename)
            else:
                self.logger.error("fields input error, please input list fields.")
                raise Exception("fields input error, please input list fields.")
        sql = sql + consql + order
        #print('select:' + sql)
        return self.executeSql(sql)

    def select_last_one(self, tablename, fields='*'):
        """倒叙查询
            args：
                tablename  ：表名字
                fields     ：检索的名称
        """

        sql = 'select %s from %s order by time desc limit 1 ' % (fields, tablename)
        #print('select:' + sql)
        if fields == "*":
            return self.executeSql(sql)
        else:
            return self.executeSql(sql)[0][0]

    def insertMany(self, table, attrs, values):
        """插入多条数据

            args：
                tablename  ：表名字
                attrs        ：属性键
                values      ：属性值

            example：
                table='test_mysqldb'
                key = ["id" ,"name", "age"]
                value = [[101, "liuqiao", "25"], [102,"liuqiao1", "26"], [103 ,"liuqiao2", "27"], [104 ,"liuqiao3", "28"]]
                mydb.insertMany(table, key, value)
        """
        self._reConn()
        values_sql = ['%s' for v in attrs]
        attrs_sql = '(' + ','.join(attrs) + ')'
        values_sql = ' values(' + ','.join(values_sql) + ')'
        sql = 'insert into %s' % table
        sql = sql + attrs_sql + values_sql
        print('insertMany:' + sql)
        try:
            # print(sql)
            for i in range(0, len(values), 20000):
                self.cur.executemany(sql, values[i:i + 20000])
                self.con.commit()
        except pymysql.Error as e:
            self.con.rollback()
            self.logger.error('in function insertMany.' + repr(e))



    def delete(self, tablename, cond_dict):
        """删除数据
            args：
                tablename  ：表名字
                cond_dict  ：删除条件字典

            example：
                params = {"name" : "caixinglong", "age" : "38"}
                mydb.delete(table, params)

        """
        self._reConn()
        consql = ' '
        if cond_dict != '':
            for k, v in cond_dict.items():
                if isinstance(v, str):
                    v = "\'" + v + "\'"
                consql = consql + tablename + "." + k + '=' + v + ' and '
        consql = consql + ' 1=1 '
        sql = "DELETE FROM %s where%s" % (tablename, consql)
        print(sql)
        return self.executeCommit(sql)

    def update(self, tablename, attrs_dict, cond_dict):
        """更新数据
            args：
                tablename  ：表名字
                attrs_dict  ：更改属性键值对字典
                cond_dict  ：更新条件字典

            example：
                params = {"name" : "caixinglong", "age" : "38"}
                cond_dict = {"name" : "liuqiao", "age" : "18"}
                mydb.update(table, params, cond_dict)
        """
        self._reConn()
        attrs_list = []
        consql = ' '
        for y in attrs_dict:
            if not isinstance(attrs_dict[y], str):
                attrs_dict[y] = str(attrs_dict[y])
        for x in cond_dict:
            if not isinstance(cond_dict[x], str):
                cond_dict[x] = str(cond_dict[x])

        for tmpkey, tmpvalue in attrs_dict.items():
            attrs_list.append("`" + tmpkey + "`" + "=" + "\'" + tmpvalue + "\'")
        attrs_sql = ",".join(attrs_list)
        #print("attrs_sql:", attrs_sql)
        if cond_dict != '':
            for k, v in cond_dict.items():
                if isinstance(v, str):
                    v = "\'" + v + "\'"
                consql = consql + "`" + tablename + "`." + "`" + k + "`" + '=' + v + ' and '
        consql = consql + ' 1=1 '
        sql = "UPDATE %s SET %s where%s" % (tablename, attrs_sql, consql)
        #print(sql)
        return self.executeCommit(sql)

    def dropTable(self, tablename):
        """删除数据库表

            args：
                tablename  ：表名字
        """
        sql = "DROP TABLE  %s" % tablename
        self.executeCommit(sql)

    def deleteTable(self, tablename):
        """清空数据库表
            args：
                tablename  ：表名字
        """
        sql = "DELETE FROM %s" % tablename
        self.executeCommit(sql)

    def isExistTable(self, tablename):
        """判断数据表是否存在
            args：
                tablename  ：表名字
            Return:
                存在返回True，不存在返回False
        """
        sql = "select * from %s" % tablename
        result = self.executeCommit(sql)
        if result is None:
            return True
        else:
            if re.search("doesn't exist", result):
                return False
            else:
                return True

    # 数据上传相关函数
    def get_devices_name(self):
        list = []
        sql = "SELECT DISTINCT device_namedevice_name FROM data_point_tbl WHERE mqtt_code != 'NULL'"
        try:
            self._reConn()
            self.cursor = self.con.cursor()
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            for row in results:
                if row[0] != None:
                    list.append(row[0])
            self.cursor.close()
            return list
        except Exception as e:
            # print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + "[ERROR] {0}\n".format(
            #     traceback.format_exc(limit=1)))
            self.logger.error('in function get_devices_name.' + repr(e))
            return False

    def get_mqtt_devices_count(self):
        list = 0
        sql = "SELECT COUNT(DISTINCT device_code) FROM data_point_tbl"
        try:
            self._reConn()
            self.cursor = self.con.cursor()
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            for row in results:
                if row[0] != None:
                    list = row[0]
            self.cursor.close()
            return list
        except:
            # print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + "[ERROR] {0}\n".format(
            #     traceback.format_exc(limit=1)))
            self.logger.error('in function get_mqtt_devices_count.' + repr(e))
            return False

    def get_mqtt_devices_from_name(self, device_name):
        list = []
        sql = "SELECT DISTINCT device_code FROM data_point_tbl WHERE device_name = '%s'" % (device_name)
        try:
            self._reConn()
            self.cursor = self.con.cursor()
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            for row in results:
                if row[0] != None:
                    list.append(row[0])
            self.cursor.close()
            return list
        except Exception as e:
            # print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + "[ERROR] {0}\n".format(
            #     traceback.format_exc(limit=1)))
            self.logger.error('in function get_mqtt_devices_from_name.' + repr(e))
            return False

    def get_mqtt_point(self, deviceCode):
        list = []
        dict = {}
        sql = "SELECT device_name,serial_number,storage_type,mqtt_code,low_limit,up_limit " \
              "FROM data_point_tbl where device_name = '%s'" % deviceCode
        try:
            self._reConn()
            self.cursor = self.con.cursor()
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            for row in results:
                dict = {}
                dict['deviceName'] = row[0]
                dict['serialNumber'] = row[1]
                dict['storageType'] = row[2]
                dict['mqttCode'] = row[3]
                dict['lowLimit'] = row[4]
                dict['upLimit'] = row[5]
                list.append(dict)
            self.cursor.close()
            return list
        except Exception as e:
            # print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + "[ERROR] {0}\n".format(
            #     traceback.format_exc(limit=1)))
            self.logger.error('in function get_mqtt_point.' + repr(e))
            return False

    def get_breakpoint_last_time_datetime(self, tableName):
        sql = "SELECT times FROM %s WHERE is_send = 0 AND times < date_sub(now(), interval 1 hour) ORDER BY id DESC LIMIT 1;" % (
            tableName)
        try:
            self._reConn()
            self.cursor = self.con.cursor(cursor=pymysql.cursors.DictCursor)
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            self.cursor.close()
            if len(results) > 0:
                return results[0]
            else:
                #self.logger.debug(results)
                return None
        except Exception as e:
            # print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + "[ERROR] {0}\n".format(
            #     traceback.format_exc(limit=1)))
            self.logger.error('in function get_breakpoint_last_time_datetime.' + repr(e))
            return None

    def get_breakpoint_last_time_date_and_time(self, tableName):
        sql = "SELECT Date,Time FROM %s WHERE is_send = 0 ORDER BY id LIMIT 1;" % (tableName)
        dict = {}
        try:
            self._reConn()
            self.cursor = self.con.cursor()
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            self.cursor.close()
            if len(results) > 0:
                dict['times'] = str(results[0][0]) + " " + str(results[0][1])
                return dict
            else:
                return None
        except Exception as e:
            # print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + "[ERROR] {0}\n".format(
            #     traceback.format_exc(limit=1)))
            self.logger.error('in function get_breakpoint_last_time_date_and_time.' + repr(e))
            return None

    def get_hour_data_datetime(self, tableName, begin, end):
        sql = "SELECT * FROM %s WHERE times >= '%s' And times <= '%s' ORDER BY id ASC;" % (tableName, begin, end)
        try:
            self._reConn()
            self.cursor = self.con.cursor(cursor=pymysql.cursors.DictCursor)
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            self.cursor.close()
            if len(results) != 0:
                return results
            else:
                return None
        except Exception as e:
            # print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + "[ERROR] {0}\n".format(
            #     traceback.format_exc(limit=1)))
            self.logger.error('in function get_hour_data_datetime.' + repr(e))
            return None

    def get_hour_data_date_and_time(self, tableName, begin, end):
        sql = "SELECT * FROM %s WHERE CONCAT(Date,' ', Time) >= '%s' And CONCAT(Date,' ', Time) <= '%s' ORDER BY id ASC;" % (
            tableName, begin, end)
        try:
            self._reConn()
            self.cursor = self.con.cursor(cursor=pymysql.cursors.DictCursor)
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            self.cursor.close()
            if len(results) != 0:
                return results
            else:
                return None
        except Exception as e:
            # print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + "[ERROR] {0}\n".format(
            #     traceback.format_exc(limit=1)))
            self.logger.error('in function get_hour_data_date_and_time.' + repr(e))
            return None

    def set_many_send_status(self, tableName, id_list):
        if len(id_list) >= 1:
            id = id_list[0]['id']
            value = 1
            sql = "UPDATE %s SET is_send = %s WHERE id = %s" % (tableName, value, id)
            for index in range(len(id_list)):
                sql = sql + " OR id = %s" % (id_list[index]['id'])

            try:
                self._reConn()
                self.cursor = self.con.cursor()
                self.cursor.execute(sql)
                # 提交到数据库执行
                self.con.commit()
                self.cursor.close()
            except Exception as e:
                # print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + "[ERROR] {0}\n".format(
                #     traceback.format_exc(limit=1)))
                self.logger.error('in function set_many_send_status.' + repr(e))

    def get_mqtt_devices(self):
        list = []
        sql = "SELECT DISTINCT device_name FROM data_point_tbl"
        try:
            self._reConn()
            self.cursor = self.con.cursor()
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            for row in results:
                if row[0] != None:
                    list.append(row[0])
            self.cursor.close()
            return list
        except Exception as e:
            # print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + "[ERROR] {0}\n".format(
            #     traceback.format_exc(limit=1)))
            self.logger.error('in function get_mqtt_devices.' + repr(e))
            return False

    def get_newest_data(self, tableName):
        sql = "SELECT * FROM %s WHERE is_send=0 ORDER BY id DESC LIMIT 1;" % (tableName)
        # sql = "SELECT * FROM %s ORDER BY id DESC LIMIT 1;" % (tableName)
        results = []
        try:
            self._reConn()
            self.cursor = self.con.cursor(cursor=pymysql.cursors.DictCursor)
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            self.cursor.close()
            if len(results) > 0:
                #print(results[0])
                return results[0]
            else:
                self.logger.debug(f"{tableName} 查询数据为0")
                return results
        except Exception as e:
            # print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + "[ERROR] {0}\n".format(
            #     traceback.format_exc(limit=1)))
            self.logger.error('in function get_newest_data.' + repr(e))
            return None

    def set_send_status(self, tableName, id, value):
        sql = "UPDATE %s SET is_send = %s WHERE id = %s;" % (tableName, value, id)
        try:
            self._reConn()
            self.cursor = self.con.cursor()
            self.cursor.execute(sql)
            # 提交到数据库执行
            self.con.commit()
            self.cursor.close()
        except Exception as e:
            # print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + "[ERROR] {0}\n".format(
            #     traceback.format_exc(limit=1)))
            self.logger.error('in function set_send_status.' + repr(e))

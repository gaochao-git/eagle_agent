import pymysql
import logging
logger = logging.getLogger('agent_logger')

class DbHelper:
    def __init__(self):
        self.mysql_host = '47.104.2.74'
        self.mysql_port = 3306
        self.mysql_user = 'gaochao'
        self.mysql_pass = 'fffjjj'
        self.mysql_db = 'eagle_agent'
        self.cursorclass = pymysql.cursors.DictCursor

    def dml(self, sql):
        conn = None
        cursor = None
        try:
            conn = pymysql.connect(host=self.mysql_host, port=self.mysql_port, user=self.mysql_user,
                                   password=self.mysql_pass,database=self.mysql_db, charset='utf8',
                                   autocommit=True)
            cursor = conn.cursor()
            cursor.execute(sql)
            return {"status": "ok", "msg": "执行成功"}
        except Exception as e:
            logger.exception(e)
            return {"status": "error", "msg": str(e)}
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    def find_all(self, sql):
        conn = None
        cursor = None
        try:
            conn = pymysql.connect(host=self.mysql_host, port=self.mysql_port, user=self.mysql_user,
                                   password=self.mysql_pass, database=self.mysql_db, charset='utf8',
                                   cursorclass=self.cursorclass)
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            return {"status": "ok", "msg": "执行成功", "data": rows}
        except Exception as e:
            logger.exception(e)
            return {"status": "error", "msg": str(e)}
        finally:
            if cursor: cursor.close()
            if conn: conn.close()
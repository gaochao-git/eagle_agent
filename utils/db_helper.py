import pymysql


class Agent:
    def __init__(self):
        self.mysql_host = '39.97.247.142'
        self.mysql_port = 3306
        self.mysql_user = 'wthong'
        self.mysql_pass = 'fffjjj'
        self.mysql_db = 'eagle_agent'
    def dml(sql):
        try:
            conn = pymysql.connect(host=self.mysql_host, port=self.mysql_port, user=self.mysql_user,
                                   password=self.mysql_pass,database=self.mysql_db, charset='utf8')
            cursor = con.cursor()
            cursor.execute(sql)
            conn.commit()
            return {"status": "ok", "msg": "执行成功"}
        except Exception as e:
            conn.rollback()
            logger.exception(e)
            return {"status": "error", "msg": str(e)}
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    def find_all(sql):
        try:
            conn = pymysql.connect(host=self.mysql_host, port=self.mysql_port, user=self.mysql_user,
                                   password=self.mysql_pass, database=self.mysql_db, charset='utf8',
                                   cursorclass=db.cursors.DictCursor)
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
#!/usr/bin/env python
# coding: utf-8

"""
设计db模块的原因：
  1. 更简单的操作数据库
      一次数据访问：   数据库连接 => 游标对象 => 执行SQL => 处理异常 => 清理资源。
      db模块对这些过程进行封装，使得用户仅需关注SQL执行。
  2. 数据安全
      用户请求以多线程处理时，为了避免多线程下的数据共享引起的数据混乱，
      需要将数据连接以ThreadLocal对象传入。

设计db接口：
  1.设计原则： 
      根据上层调用者设计简单易用的API接口
  2. 调用接口
      1. 初始化数据库连接信息
          from transwarp import db
          db.create_engine(user='root', 
                           password='password',
                           database='test',
                           host='127.0.0.1',
                           port=3306)

      2. 执行查询
          users = db.select('select * from user')
          # users =>
          # [
          #     { "id": 1, "name": "Michael"},
          #     { "id": 2, "name": "Bob"},
          #     { "id": 3, "name": "Adam"}
          # ]
"""

import time, uuid, functools
import threading
import logging


# global engine object:
engine = None



def next_id(t=None):
    """
    生成一个唯一id   由 当前时间 + 随机数（由伪随机数得来）拼接得到
    """
    if t is None:
        t = time.time()
    return "%015d%s000" % (int(t * 1000), uuid.uuid4().hex)

def _profiling(start, sql=''):
    """
    用于剖析sql的执行时间
    """
    t = time.time() - start
    if t > 0.1:
        logging.warning("[PROFILING] [DB] %s: %s" % (t, sql))
    else:
        logging.info("[PROFILING] [DB] %s: %s" % (t, sql))

def create_engine(user, password, database, host='127.0.0.1', port=3306, **kw):
    """
    db模型的核心函数，用于连接数据库, 生成全局对象engine，
    engine对象持有数据库连接
    """
    import mysql.connector
    global engine
    if engine is not None:
        raise DBError('Engine is already initialized.')
    params = dict(user=user, password=password, database=database, host=host, port=port)
    defaults = dict(use_unicode=True, charset='utf-8', collation='utf8_general_ci', autocommit=False)
    for k, v in defaults.iteritems():
        params[k] = kw.pop(k, v)
    params.update(kw)
    params['buffered'] = True
    engine = _Engine(lambda: mysql.connector.connect(**params))
    logging.info('Init mysql engine <%s> ok.' % hex(id(engine)))

def connection():
    """
    db模块核心函数，用于获取一个数据库连接
    通过_ConnectionCtx对 _db_ctx封装，使得惰性连接可以自动获取和释放，
    也就是可以使用 with语法来处理数据库连接
    _ConnectionCtx    实现with语法
    ^
    |
    _db_ctx           _DbCtx实例
    ^
    |
    _DbCtx            获取和释放惰性连接
    ^
    |
    _LasyConnection   实现惰性连接
    """
    return _ConnectionCtx()


class Dict(dict):
    """
    字典对象
    实现一个简单的可以通过属性访问的字典，比如 x.key = value
    """
    def __init__(self, **kwargs):
        super(Dict, self).__init__(**kwargs)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
       self[key] = value


class DBError(Exception):
    """
    DBError exception object
    """
    pass


class MultiColumnError(DBError):
    """
    MultiColumnError exception object
    """
    pass


class _Engine(object):
    """
    数据库引擎对象
    用于保存 db模块的核心函数：create_engine 创建出来的数据库连接
    """
    def __init__(self, connect):
        self._connect = connect

    def connect(self):
        return self._connect


class _LasyConnection(object):
    """
    惰性连接对象 
    仅当需要cursor对象时，才连接数据库，获取连接 
    """
    def __init__(self):
        self.connect = None

    def cursor(self):
        if self.connect is None:
            connection = engine.connect()
            logging.info("open connection <%s>..." % hex(id(connection)))
            self.connection = connection
        return self.connection.cursor()

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def cleanup(self):
        if self.connection:
            connection = self.connection
            self.connection = None
            logging.info("close connection <%s>..." % hex(id(connection)))
            connection.close()


class _DbCtx(threading.local):
    """
    db模块的核心对象, 数据库连接的上下文对象，负责从数据库获取和释放连接
    取得的连接是惰性连接对象，因此只有调用cursor对象时，才会真正获取数据库连接
    该对象是一个 Thread local对象，因此绑定在此对象上的数据 仅对本线程可见
    """
    def __init__(self):
        self.connection = None
        self.transactions = 0

    def is_init(self):
        """
        返回一个布尔值，用于判断 此对象的初始化状态
        """
        return not self.connection is None

    def init(self):
        """
        初始化连接的上下文对象，获得一个惰性连接对象
        """
        logging.info('open lazy connection...')
        self.connection = _LasyConnection()
        self.transactions = 0

    def cleanup(self):
        """
        清理连接对象，关闭连接
        """
        self.connection.cleanup()
        self.connection = None

    def cursor(self):
        """
        获取cursor对象， 真正取得数据库连接 
        """
        return self.connection.cursor()

# thread-local db context:
_db_ctx = _DbCtx()

class _ConnectionCtx(object):
    """
    因为_DbCtx实现了连接的 获取和释放，但是并没有实现连接
    的自动获取和释放，_ConnectCtx在 _DbCtx基础上实现了该功能，
    因此可以对 _ConnectCtx 使用with 语法，比如：
    with connection():
        pass
        with connection():
            pass 
    """
    def __enter(self):
        """
        获取一个惰性连接对象 
        """
        global _db_ctx
        self.should_cleanup = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_clean = True

    def __exit__(self, exctype, excvalue, traceback):
        """
        释放连接
        """
        global _db_ctx
        if self.should_cleanup:
            _db_ctx.cleanup()





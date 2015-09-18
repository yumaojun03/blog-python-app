#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
实现以Json数据格式进行交换的RESTful API
设计原因：
    由于API就是把Web App的功能全部封装了，所以，通过API操作数据，
    可以极大地把前端和后端的代码隔离，使得后端代码易于测试，
    前端代码编写更简单
实现方式：
    一个API也是一个URL的处理函数，我们希望能直接通过一个@api来
    把函数变成JSON格式的REST API， 因此我们需要实现一个装饰器，
    由该装饰器将 函数返回的数据 处理成 json 格式
"""

import json
import logging
import functools

from transwarp.web import ctx


def dumps(obj):
    """
    Serialize ``obj`` to a JSON formatted ``str``.
    序列化对象
    """
    return json.dumps(obj)


class APIError(StandardError):
    """
    the base APIError which contains error(required), data(optional) and message(optional).
    存储所有API 异常对象的数据
    """
    def __init__(self, error, data='', message=''):
        super(APIError, self).__init__(message)
        self.error = error
        self.data = data
        self.message = message


class APIValueError(APIError):
    """
    Indicate the input value has error or invalid. The data specifies the error field of input form.
    输入不合法 异常对象
    """
    def __init__(self, field, message=''):
        super(APIValueError, self).__init__('value:invalid', field, message)


class APIResourceNotFoundError(APIError):
    """
    Indicate the resource was not found. The data specifies the resource name.
    资源未找到 异常对象
    """
    def __init__(self, field, message=''):
        super(APIResourceNotFoundError, self).__init__('value:notfound', field, message)


class APIPermissionError(APIError):
    """
    Indicate the api has no permission.
    权限 异常对象
    """
    def __init__(self, message=''):
        super(APIPermissionError, self).__init__('permission:forbidden', 'permission', message)


def api(func):
    """
    A decorator that makes a function to json api, makes the return value as json.
    将函数返回结果 转换成json 的装饰器
    @api需要对Error进行处理。我们定义一个APIError，
    这种Error是指API调用时发生了逻辑错误（比如用户不存在）
    其他的Error视为Bug，返回的错误代码为internalerror

    @app.route('/api/test')
    @api
    def api_test():
        return dict(result='123', items=[])
    """
    @functools.wraps(func)
    def _wrapper(*args, **kw):
        try:
            r = dumps(func(*args, **kw))
        except APIError, e:
            r = json.dumps(dict(error=e.error, data=e.data, message=e.message))
        except Exception, e:
            logging.exception(e)
            r = json.dumps(dict(error='internalerror', data=e.__class__.__name__, message=e.message))
        ctx.response.content_type = 'application/json'
        return r
    return _wrapper

if __name__ == '__main__':
    import doctest
    doctest.testmod()


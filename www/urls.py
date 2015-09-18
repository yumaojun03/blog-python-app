#!/usr/bin/env python
# -*- coding: utf-8 -*-


import os, re, time, base64, hashlib, logging

from transwarp.web import get, post, ctx, view, interceptor, HttpError

from models import User, Blog, Comment

@view('blogs.html')
@get('/')
def index():
    blogs = Blog.find_all()
    user = User.find_first('where email=?', 'admin@example.com')
    return dict(blogs=blogs, user=user)

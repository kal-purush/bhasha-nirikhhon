#!/usr/bin/env python
from flask import g, jsonify, Blueprint, request, abort, current_app
from werkzeug.security import generate_password_hash, check_password_hash
import datetime
from sqlalchemy.sql import *
import json
import jwt
import random
import requests


from src.models import db, session_scope, user_source, Channel,ThirdDateLog
from src.services import send_email, redis_store
import hashlib
from src.services import classin_service,do_query


BEARER_TOKEN = 'Bearer '

auth = Blueprint('channel', __name__)


@auth.route('/login', methods=['POST'])
def login():
    """
    swagger-doc: 'do login for registered user'
    required: ['username', 'password', 'usertype']
    req:
      username:
        description: 'login user name'
        type: 'string'
      password:
        description: 'login user password'
        type: 'string'
      partnerId:
        description: '给对方的key'
        type: 'string'
      timeStamp:
        description: '当前时间秒'
        type: 'string'
      appKey:
        description: 'MD5(secret+timeStamp)'
        type: 'string'
      token:
        description: '返回需要携带的信息'
        type: 'string'
      sex:
        description: '性别 ：male或female'
        type: 'string'
    res:
      Authorization:
        description: 'Athorization jwt http header'
        type: 'string'
    """
    user_name = request.json['mobile']
    code = request.json['code']
    password = request.json['password']
    user_type = 'Student'
    partnerId = request.json['partnerId']
    appKey = request.json['appKey']
    timeStamp = request.json['timeStamp']
    target_table = user_source[user_type]
    real_name = request.json['real_name']
    token = request.json['token']
    sex1= request.json['sex']
    sex = '男';
    if sex1 == 'female':
            sex = '女';


    with session_scope(db) as session:

        channel = session.query(Channel).filter_by(partner_id=partnerId).one_or_none()
        if channel is None :
            return jsonify({
                "error": "not found Channel: {0}".format(
                    partnerId)
            }), 500

        m2 = hashlib.md5()
        m2.update(channel.app_key+timeStamp)

        if m2.hexdigest() != appKey:
            if channel is None :
                return jsonify({
                    "error": "appKey error: {0}".format(
                        partnerId)
                }), 500

        user = session.query(user_source[user_type]).filter_by(
            username=user_name).first()

        if user is None:
            user = target_table(username=user_name,
                                password=generate_password_hash(request.json['password']), state=1,
                                updated_by=user_name, mobile=user_name,nickname=user_name,name=real_name,nation = code,gender=sex)

            session.add(user)
            session.flush()
        if user:
            # check password
            if check_password_hash(getattr(user, 'password', ''), password):
                token = generate_jwt_token(current_app.config['JWT_HEADER'],
                                           user_name)
                thirdDateLog = ThirdDateLog(table_name = 'Channel',
                                           table_id = appKey,
                                           third_id = user.username,
                                            third_date = token,
                                           delete_flag = 'IN_FORCE')
                session.add(thirdDateLog)
                session.flush()

                return jsonify({**token})
            else:
                return jsonify({'message': 'password check failed!'}), 401
        else:
            return jsonify({'message': user_name + ' not found!'}), 401


def generate_jwt_token(header_name, username):
    payload = {
        current_app.config['JWT_SUBJECT_KEY']: username,
        'exp': datetime.datetime.utcnow() + datetime.timedelta(days=1),
        'iat': datetime.datetime.utcnow(),
        'iss': 'src'
    }
    encoded = jwt.encode(payload, current_app.config['JWT_SECRET'],
                         algorithm=current_app.config['JWT_ALG'])
    return {header_name: BEARER_TOKEN + str(encoded, encoding='utf-8')}
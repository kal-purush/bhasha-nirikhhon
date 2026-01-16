from flask import Flask, request, make_response, jsonify, url_for
from celery import Celery
from celery.utils.log import get_task_logger
from urlparse import urlparse

import requests as r

from .config import Default

app = Flask(__name__)
app.config.from_object(Default)
app.config.from_envvar('APP_SETTINGS', Default)

CELERY_DISABLE_RATE_LIMITS = True
VALID_SCHEMES = [ 'http', 'https' ]

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)

from .validate import async_validate
from .publish import publish as _publish

logger = get_task_logger(__name__)

@celery.task(bind=True)
def validate(self, source, target):
    logger.info('validate with source="{0}", target="{1}"'.format(source, target))
    return async_validate(self, source, target)

@celery.task(bind=True)
def publish(self, args):
    logger.info('Publishing with args: {0}'.format(args))
    if args['status'] == 'VALID':
        source, target = args['source'], args['target']
        return _publish(source, target)

@celery.task(bind=True)
def invalid(self, uuid):
    result = self.app.AsyncResult(uuid)
    self.update_state(meta={'info': str(result.result)})
    print('Task failure: {0}: {1}'.format(type(result.result), result.result))
    return { 'result': str(result.result) }
    

validate_and_publish = validate.subtask(link=publish.s(), link_error=invalid.s())

@app.route('/webmention', methods=['POST'])
def webmention():
    try:
        source = request.form['source']
    except KeyError:
        return make_response('no source defined', 400)

    try:
        target = request.form['target']
    except KeyError:
        return make_response('no target defined', 400)

    uparts = urlparse(target)
    if uparts.scheme not in VALID_SCHEMES:
        return make_response('unhandled scheme', 400)

    uparts = urlparse(source)
    if uparts.scheme not in VALID_SCHEMES:
        return make_response('unhandled scheme', 400)

    if target == source:
        return make_response('source and target must be different', 400)
    
    # generate status url
    task = validate_and_publish.delay(source, target)
    #task = validate.delay(source, target)
    status_url = url_for('taskstatus', task_id=task.id)
    response = {
        'status': 'queued',
        'summary': 'Webmention was queued for processing',
        'location': status_url
    }
    return jsonify(response), 201, {'Location': status_url }

@app.route('/webmention/<task_id>')
def taskstatus(task_id):
    task = validate_and_publish.AsyncResult(task_id)

    if task.state != 'SUCCESS':
        task = invalid.AsyncResult(task_id)
        #print('task not SUCCESS, invalid.state: {0}, info: {1}'.format(task.state, type(task.info)))
        info = str(task.info)
    else:
        info = task.info
        
    response = {
        'state': task.state,
        'info': info
    }

    try:
        json = jsonify(response)
    except TypeError:
        print('could not jsonify response: {0}'.format(response))
        return jsonify({ 'error': 'could not jsonify response' }), 500
    else:    
        return jsonify(response), 200
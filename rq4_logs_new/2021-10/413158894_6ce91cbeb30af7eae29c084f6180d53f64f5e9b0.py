from typing import (
    List
)

from flask_restx import (
    Namespace,
    Resource
)

api = Namespace('hfc', 'HFC Functions')


@api.route('/')
class HFC(Resource):
    @api.doc('get')
    def get(self):
        return {'hfc': 'test'}


@api.route('/list')
class HFCList(Resource):
    @api.doc('get')
    def get(self):
        _list: List[str] = [
            'Test',
            'Test2',
            'Test3',
        ]
        return _list

    @api.doc('post')
    def post(self):
        return {'method': 'POST'}
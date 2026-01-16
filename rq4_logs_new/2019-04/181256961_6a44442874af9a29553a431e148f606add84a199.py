from App.DB import *


class CiModel:

    @staticmethod
    def all():
        return DB.selectAll(['id', 'transport_id', 'cliente_id'], 'commercial_invoice')

    @staticmethod
    def create(data):
        return DB.insert(data, 'commercial_invoice')

    @staticmethod
    def get(ci_model_id):
        return DB.SelectById(ci_model_id, ['id', 'transport_id', 'client_id'], 'commercial_invoice')

    @staticmethod
    def remove(ci_model_id):
        return DB.remove(ci_model_id, 'commercial_invoice')
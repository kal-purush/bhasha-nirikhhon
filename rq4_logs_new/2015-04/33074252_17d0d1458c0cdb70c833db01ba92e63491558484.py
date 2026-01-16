class Database:
    def __init__(self):
        self.con = 'connect database'

    @staticmethod
    def setup():
        try:
            print('t')
        except Exception as e:
            return {'error': e.args[1]}
        else:
            return {'success': "Banco de dados configurado com sucesso"}

    @staticmethod
    def connect():
        try:
            con = {}
        except Exception as e:

            return None
        else:
            return con
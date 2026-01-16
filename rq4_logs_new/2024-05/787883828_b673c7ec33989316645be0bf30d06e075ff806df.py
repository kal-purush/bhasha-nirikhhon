from db import mongo_client
from helper import get_valid_end_date


class SalaryAggregator:
    """
    Класс для сбора статистических данных о зарплатах
    сотрудников компании по временным промежуткам.
    """
    def __init__(self, collection):
        self.collection = collection
        # Словарь с настройками данных для разных типов агрегации.
        self.settings = {
            'day': {
                'date_format': '%Y-%m-%d',
                'time_format': 'T00:00:00',
                'dateToString': {'format': '%Y-%m-%d', 'date': '$dt'},
            },
            'hour': {
                'date_format': '%Y-%m-%dT%H',
                'time_format': ':00:00',
                'dateToString': {'format': '%Y-%m-%dT%H', 'date': '$dt'}
            },
            'month': {
                'date_format': '%Y-%m',
                'time_format': '-01T00:00:00',
                'dateToString': {'format': '%Y-%m', 'date': '$dt'}
            }
        }

    async def aggregate_data(self, start_date, end_date, aggregation_type):
        """Метод для получения суммы всех выплат в заданном диапазоне."""
        valid_end_date = get_valid_end_date(end_date, aggregation_type)

        pipeline = [
            {
                '$match': {
                    'dt': {'$gte': start_date, '$lte': end_date}
                }
            },
            {
                '$densify': {
                    'field': 'dt',
                    'range': {
                        'step': 1,
                        'unit': aggregation_type,
                        'bounds': [start_date, valid_end_date]
                    }
                }
            },
            {
                '$group': {
                    '_id': {'$dateToString': self.settings[aggregation_type]['dateToString']},
                    'total': {'$sum': '$value'}
                }
            },
            {
                '$sort': {'_id': 1}
            }
        ]

        result = {'dataset': [], 'labels': []}
        cursor = await self.collection.aggregate(pipeline).to_list(length=None)

        result['dataset'] = [doc['total'] for doc in cursor]
        result['labels'] = [doc['_id'] + self.settings[aggregation_type]['time_format'] for doc in cursor]

        return result


collection = mongo_client.get_collection()
aggregator = SalaryAggregator(collection)
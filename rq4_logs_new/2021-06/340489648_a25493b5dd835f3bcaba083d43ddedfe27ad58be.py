import datetime
import json


class MockDataSet:

    def __init__(self, num_results):
        self.start = datetime.datetime(2020, 1, 1, 0, 0, 0, 0)
        self.delta = datetime.timedelta(seconds=1)
        self.counter = 1
        self.num_results = num_results
        self.results = []

    def get_results(self):

        while self.num_results > 5000:
            self.make_full_pages_of_results(count=20,
                                            total_count=self.num_results)
            self.num_results -= 5000
        self.make_full_pages_of_results(count=self.num_results // 250,
                                        total_count=self.num_results)
        self.num_results -= (self.num_results // 250) * 250
        self.make_partial_page_of_results(result_count=self.num_results,
                                          total_count=650)

        return self.results

    def make_full_pages_of_results(self, count, total_count):
        for result_count in range(1, count + 1):
            data = []
            for _ in range(250):
                attributes = {
                    'lastModifiedDate': (
                        (self.start + self.delta * self.counter).strftime(
                            '%Y-%m-%dT%H:%M:%SZ'))
                }
                data.append({
                    'id': self.counter,
                    'attributes': attributes
                })
                self.counter += 1
            meta = {
                'totalElements': total_count,
                'pageNumber': result_count,
                'totalPages': 20
            }

            self.results.append(
                {'text': json.dumps({'data': data, 'meta': meta})})

    def make_partial_page_of_results(self, result_count, total_count):
        data = []
        for _ in range(result_count):
            date_str = (self.start + self.delta * self.counter).strftime(
                '%Y-%m-%dT%H:%M:%SZ')
            attributes = {
                'lastModifiedDate': date_str
            }
            data.append({
                'id': self.counter,
                'attributes': attributes
            })
            self.counter += 1
        meta = {
            'totalElements': total_count,
            'pageNumber': 3,
            'totalPages': 3
        }

        self.results.append({'text': json.dumps({'data': data, 'meta': meta})})
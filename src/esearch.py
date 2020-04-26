from elasticsearch import Elasticsearch
import json


class ESearchUnit:
    def __init__(self):
        self.es = Elasticsearch()
        self.index = None

        self.INSERT_SUCCESS = 'created'
        self.DELETE_SUCCESS = 'deleted'

    def set_index(self, index: str):
        self.index = index

    def creat_index(self, index: str):
        r = self.es.indices.create(index=index, ignore=400)
        print(r)

    def del_index(self, index: str):
        r = self.es.indices.delete(index=index, ignore=[400, 404])
        print(r)

    def insert(self, data: dict, index=None):
        if index is None:
            if self.index is None:
                print('[ES ERROR]: valid index.')
                return False
            index = self.index

        r = self.es.index(index=index, body=data)
        if r['result'] == self.INSERT_SUCCESS:
            return True
        else:
            return False

    def search(self, dsl: dict):
        result = self.es.search(index=self.index, body=dsl)
        return result


def es_storage(data):
    es_index = 'bigdata1'
    es = ESearchUnit()
    es.del_index(es_index)
    es.creat_index(es_index)
    es.set_index(es_index)
    for item in data:
        try:
            es.insert(data=item, index=es_index)
        except:
            continue
    with open('output.txt', 'w') as f:
        dsl = {
            'query': {
                'match_all': {}
            },
            'size': 100
        }
        json.dump(obj=es.search(dsl), fp=f)

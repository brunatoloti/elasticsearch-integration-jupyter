import os
import json
import logging
import pandas as pd
from dotenv import load_dotenv
from elasticsearch import helpers
from elasticsearch import Elasticsearch


def get_data():
    df = pd.read_csv("association-models.csv")
    #Converting the data into the format that Elasticsearch can understand
    dic = df.to_dict("records")
    return dic


def connect_elasticsearch():
    _es = None
    load_dotenv()
    ENDPOINT = os.getenv('ENDPOINT')
    _es = Elasticsearch(timeout=600, hosts=ENDPOINT)
    if _es.ping():
        print("Connected!")
    else:
        raise ValueError("Connection failed!")
    return _es


def create_index(es_object, index_name):
    created = False
    mapping = {
        "mappings" : {
        "properties" : {
            "conditional_probability" : {
            "type" : "float"
            },
            "count_have" : {
            "type" : "long"
            },
            "count_old_atual" : {
            "type" : "long"
            },
            "have" : {
            "type" : "text",
            "fields" : {
                "keyword" : {
                "type" : "keyword",
                "ignore_above" : 256
                }
            }
            },
            "have_want" : {
            "type" : "text",
            "fields" : {
                "keyword" : {
                "type" : "keyword",
                "ignore_above" : 256
                }
            }
            },
            "want" : {
            "type" : "text",
            "fields" : {
                "keyword" : {
                "type" : "keyword",
                "ignore_above" : 256
                }
            }
            }
        }
        }
    }
    
    try:
        if not es_object.indices.exists(index_name):
            es_object.indices.create(index=index_name, ignore=[400, 404], body=mapping)
            print("Created Index!")
        created = True
    except Exception as e:
        print(str(e))
    finally:
        return created


def upload_data(es, dic):
    try:
        res = helpers.bulk(es, generator(dic))
        print("Uploaded!")
    except Exception as e:
        print(e)
        pass


def generator(dictionary):
    for i, line in enumerate(dictionary):
        yield {
            '_index': 'test-upload',
            '_type': '_doc',
            '_id': i,
            '_source': {
                'have': line.get("have", ""),
                'want': line.get("want", ""),
                'have_want': line.get("have_want", ""),
                'count_old_atual': line.get("count_old_atual", ""),
                'count_have': line.get("count_have", ""),
                'conditional_probability': line.get("condicional_probability", "")
            }
        }


if __name__ == "__main__":
    dic = get_data()
    es = connect_elasticsearch()
    create_index(es, 'test-upload')
    upload_data(es, dic)
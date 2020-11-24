import os
import json
import logging
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from elasticsearch import helpers
from elasticsearch import Elasticsearch


def get_data():
    FILE_PATH = Path("../Data", "books.csv")
    df = pd.read_csv(FILE_PATH, error_bad_lines=False)
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
                "average_rating" : {
                "type" : "float"
                },
                "num_pages" : {
                "type" : "long"
                },
                "ratings_count" : {
                "type" : "long"
                },
                "text_reviews_count" : {
                "type" : "long"
                },
                "title" : {
                "type" : "text",
                "fields" : {
                    "keyword" : {
                    "type" : "keyword"
                    }
                }
                },
                "authors" : {
                "type" : "text",
                "fields" : {
                    "keyword" : {
                    "type" : "keyword",
                    "ignore_above" : 256
                    }
                }
                },
                "isbn" : {
                "type" : "text",
                "fields" : {
                    "keyword" : {
                    "type" : "keyword",
                    "ignore_above" : 256
                    }
                }
                },
                "language_code" : {
                "type" : "text",
                "fields" : {
                    "keyword" : {
                    "type" : "keyword",
                    "ignore_above" : 256
                    }
                }
                },
                "publication_date" : {
                "type" : "text",
                "fields" : {
                    "keyword" : {
                    "type" : "keyword",
                    "ignore_above" : 256
                    }
                }
                },
                "publisher" : {
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
            '_index': 'books',
            '_type': '_doc',
            '_id': line.get("bookID", ""),
            '_source': {
                'title': line.get("title", ""),
                'authors': line.get("authors", ""),
                'average_rating': line.get("average_rating", ""),
                'isbn': line.get("isbn", ""),
                'language_code': line.get("language_code", ""),
                'num_pages': line.get("  num_pages", ""),
                'ratings_count': line.get("ratings_count", ""),
                'text_reviews_count': line.get("text_reviews_count", ""),
                'publication_date': line.get("publication_date", ""),
                'publisher': line.get("publisher", "")
            }
        }


if __name__ == "__main__":
    dic = get_data()
    es = connect_elasticsearch()
    create_index(es, 'books')
    upload_data(es, dic)
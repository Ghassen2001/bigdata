import os
import json
from elasticsearch import Elasticsearch, helpers

ELASTIC_HOST = os.environ.get("ELASTIC_HOST", "elasticsearch")
ELASTIC_PORT = os.environ.get("ELASTIC_PORT", "9200")
ELASTIC_INDEX = os.environ.get("ELASTIC_INDEX", "retail_data")
DATA_PATH = os.path.join(os.path.dirname(__file__), '../data/processed_data.json')

es = Elasticsearch([{'host': ELASTIC_HOST, 'port': ELASTIC_PORT}])

def load_data(path):
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            yield json.loads(line)

def index_data():
    actions = [
        {
            "_index": ELASTIC_INDEX,
            "_source": doc
        }
        for doc in load_data(DATA_PATH)
    ]
    if actions:
        helpers.bulk(es, actions)
        print(f"✅ {len(actions)} documents indexés dans Elasticsearch.")
    else:
        print("Aucune donnée à indexer.")

if __name__ == "__main__":
    index_data()

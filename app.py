import pandas as pd
from flask import Flask, jsonify, render_template
from elasticsearch import Elasticsearch
import json

app = Flask(__name__)

# Elasticsearch bağlantısını kurma
es = Elasticsearch("http://0.0.0.0:9200")

@app.route('/data', methods=['GET'])
def get_data():
    # Elasticsearch'den veri sorgulama
    response = es.search(index="jaeger-span-2024-07-12", body={
        "query": {
            "bool": {
                "must": [
                    {"match": {"operationName": "POST /api/transaction"}},
                    {"match": {"process.serviceName": "unknown_service:node"}},
                    {
                        "nested": {
                            "path": "tags",
                            "query": {
                                "bool": {
                                    "must": [
                                        {"match": {"tags.key": "http.response.post"}}
                                    ]
                                }
                            }
                        }
                    }
                ]
            }
        },
        "_source": ["operationName", "tags"],
        "from": 0,
        "size": 1000
    })
    
    # Tags alanından istenen verileri filtreleme
    data = []
    for hit in response['hits']['hits']:
        tags = hit['_source']['tags']
        for tag in tags:
            if tag['key'] == 'http.response.post':
                response_body = json.loads(tag['value'])
                source_account = response_body['User']
                destination_account = response_body['User2']

                # Source account data (harcanan para)
                data.append({
                    'userId': source_account['user_id'],
                    'userName': source_account['name'],
                    #'accountNumber': source_account['accountNumber'],
                    'total_spent': response_body['amount'],
                    'total_received': 0,
                    'transaction_count': 1
                })
                
                # Destination account data (alınan para)
                data.append({
                    'userId': destination_account['user_id'],
                    'userName': destination_account['name'],
                    #'accountNumber': destination_account['accountNumber'],
                    'total_spent': 0,
                    'total_received': response_body['amount'],
                    'transaction_count': 1
                })
    
    # Pandas ile veriyi işleme
    if data:
        df = pd.DataFrame(data)
        
        # Toplam harcanan ve alınan para miktarlarını ve işlem sayısını gruplama
        summary = df.groupby(['userId', 'userName']).agg({
            'total_spent': 'sum',
            'total_received': 'sum',
            'transaction_count': 'sum'
        }).reset_index()

        
        return jsonify(summary.to_dict(orient='records'))
    else:
        return jsonify([])
"""
@app.route('/')
def index():
    return render_template('index.html')
"""
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5009, debug=False)

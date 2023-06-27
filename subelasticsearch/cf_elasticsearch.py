from elasticsearch import Elasticsearch
import json, time

# Kết nối tới Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])
def create_index_es(index_name):
    body = {
        "mappings": {
            "properties": {
                "created_at": {
                    "type": "date"
                }
            }
        }
    }
    response = es.indices.create(index=index_name, body=body)
    # Kiểm tra kết quả
    if response['acknowledged']:
        print(f"Chỉ mục {index_name} đã được tạo thành công.")
    else:
        print(f"Lỗi khi tạo chỉ mục {index_name}.")

class DataElasticsearch():
    def __init__(self, index_name):
        self.index_name = index_name  # Tên chỉ mục bạn muốn lưu trữ dữ liệu

    def send_data(self, data):
        # Chuyển đổi dữ liệu thành định dạng JSON
        json_data = json.dumps(data)
        response = es.index(index=self.index_name, body=json_data)

        time.sleep(1)
        # Kiểm tra kết quả
        if response['result'] == 'created':
            print('Dữ liệu đã được truyền thành công vào Elasticsearch.')
        else:
            print('Lỗi khi truyền dữ liệu vào Elasticsearch.')

    def get_data(self):
        # Truy vấn tài liệu mới đưa vào Elasticsearch
        query = {
            "query": {
                "match_all": {}
            },
            "sort": [
                {"created_at": {"order": "desc"}}
            ],
            "size": 10
        }


        # Gửi truy vấn và nhận kết quả
        response = es.search(
            index=self.index_name, 
            body=query
        )

        # Xử lý kết quả
        if response['hits']['total']['value'] > 0:
            print("Có", response['hits']['total']['value'], "tài liệu mới được đưa vào Elasticsearch:")
            for hit in response['hits']['hits']:
                print(hit['_id'], hit['_source'])
        else:
            print("Không có tài liệu mới trong Elasticsearch.")
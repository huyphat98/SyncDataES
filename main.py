from database.pgcrud import create_data, read_data, update_data, delete_data
from subelasticsearch.cf_elasticsearch import DataElasticsearch, create_index_es

########## Test Crud multiple table postgres ##########
# print(create_data('items', **{
#   "id": 2,
#   "name": "tttt",
#   "description": "vvvv",
#   "price": 0,
#   "on_offer": True
# }))
# print(read_data('items'))
# print(delete_data('items', 1))
# print(update_data('items', 2, **{
#     'name': 'test',
#     'price': 100
# }))


########## Test Elasticsearch ##########
# index_name = create_index_es('my_index')
data = read_data('items')
print(data)
els = DataElasticsearch('my_index')
els.send_data(data['data_table'])
els.get_data()


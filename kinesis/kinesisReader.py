from pymodm import MongoModel, fields, connect


clctn = connect('mongodb://mohammade:<PASSWORD>@tega-test-shard-00-00-z0auy.mongodb.net:27017,tega-test-shard-00-01-z0auy.mongodb.net:27017,tega-test-shard-00-02-z0auy.mongodb.net:27017/kinesis-test-data?ssl=true&replicaSet=tega-test-shard-0&authSource=admin',
                alias='mytest')
# mongo_db = con[]


class DummyUser(MongoModel):
    firstname = fields.CharField()
    lastname = fields.CharField()
    age = fields.IntegerField()
    gender = fields.CharField()
    job = fields.CharField()
    address = fields.CharField()
    email = fields.EmailField()

    class Meta:
        connection_alias = 'mytest'
    #     write_concern = WriteConcern(j=True)


import boto3
import json
import datetime
import time
import pytz
from dateutil.tz import tzlocal

stream_name = 'my-dummy-test'
session = boto3.Session(profile_name='default')
kinesis_client = session.client('kinesis', region_name='ap-southeast-2')
stream_desc = kinesis_client.describe_stream(StreamName=stream_name)
shards_count = len(stream_desc['StreamDescription']['Shards'])

shard_id = stream_desc['StreamDescription']['Shards'][0]['ShardId']

shard_iterator = kinesis_client.get_shard_iterator(StreamName=stream_name,
                                                   ShardId=shard_id,
                                                   ShardIteratorType='LATEST')

my_shard_iterator = shard_iterator['ShardIterator']

record_response = kinesis_client.get_records(ShardIterator=my_shard_iterator,
                                             Limit=2)

while 'NextShardIterator' in record_response:
    record_response = kinesis_client.get_records(ShardIterator=record_response['NextShardIterator'],
                                                 Limit=2)
    # record_response = {'Records': [{'SequenceNumber': '49578353737063758287189849966438150426880016146686279682', 'ApproximateArrivalTimestamp': datetime.datetime(2017, 10, 28, 22, 1, 45, 23000, tzinfo=tzlocal()), 'Data': b'{"first_name": "Daniel", "last_name": "King", "age": 87, "gender": "female", "job": "Designer, jewellery", "address": "153 Lee Estates Apt. 061\\nSouth Richard, MI 12756-7497", "email": "deborahpadilla@king.net"}', 'PartitionKey': 'aa'}], 'NextShardIterator': 'AAAAAAAAAAGuhw7mmeLUy9sJzkGp7jqzg8CWRAJmWXNp7FSMbbVQaPNEYcrg7k7N6ik+j09Rygb1olJ3PHd77qaRk5UpN1BRNM+K54EjakfMCKmFtmUnJ4SlYYYwoqAusvor8ndG6Wyyf9595kH64JhRGM/XOuOqilxwJQoRLR89lOhlUmBUYch/WQF3schQChP52Yscr79vyp13GG7Xosh+EghbUdw3', 'MillisBehindLatest': 0, 'ResponseMetadata': {'RequestId': 'ec2ddcdc-ade0-c12a-be43-b900f3d7aef9', 'HTTPStatusCode': 200, 'HTTPHeaders': {'server': 'Apache-Coyote/1.1', 'x-amzn-requestid': 'ec2ddcdc-ade0-c12a-be43-b900f3d7aef9', 'x-amz-id-2': 'Bk2I7YKEfY84kfbNM+PaSsLVHkL+vtF1sFrJpc6Wt9aiOAhw2dqBFLcS+pohLyja7LPzUzVgC4l466ylTPNdS5LwcqYUETLV', 'content-type': 'application/x-amz-json-1.1', 'content-length': '722', 'date': 'Sat, 28 Oct 2017 11:01:47 GMT'}, 'RetryAttempts': 0}}
    print(record_response)

    if len(record_response['Records']) > 0:
        if record_response['Records'][0]['Data']:
            print('INserting into mongo')
            record_data = json.loads(record_response['Records'][0]['Data'])
            observ = DummyUser(firstname=record_data['first_name'],
                               lastname=record_data['last_name'],
                               age=record_data['age'],
                               gender=record_data['gender'],
                               job=record_data['job'],
                               address=record_data['address'],
                               email=record_data['email'],
                               ).save()

    # wait for 5 seconds
    time.sleep(5)

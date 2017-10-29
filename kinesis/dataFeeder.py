import boto3
import json
from faker import Faker
import time
import datetime as dt
import user_maker as um

stream_name = 'my-dummy-test'
session = boto3.Session(profile_name='default')
kinesis_client = session.client('kinesis', region_name='ap-southeast-2')
kinesis_client.create_stream(StreamName=stream_name,
                             ShardCount=2)



while kinesis_client.describe_stream(StreamName=stream_name)['StreamDescription']['StreamStatus'] != 'ACTIVE':
    time.sleep(5)


fake = Faker()

def put_to_stream(stream, part_key):
    user = um.User(
        first_name=fake.first_name(),
        last_name=fake.last_name(),
        age=fake.random_int(18, 90),
        gender=fake.random_element(['female', 'male']),
        job=fake.job(),
        address=fake.address(),
        email=fake.email()
    )

    print(user)

    put_response = kinesis_client.put_record(
        StreamName=stream,
        Data=json.dumps(user.__dict__),
        PartitionKey=part_key
    )
    return put_response

while True:

    part_name = 'aa'

    put_to_stream(stream=stream_name,
                  part_key=part_name)
    time.sleep(10)


kinesis_client.delete_stream(StreamName=stream_name)
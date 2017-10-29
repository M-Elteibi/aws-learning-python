from boto import kinesis
import testdata
import json

kinesis = kinesis.connect_to_region(region_name="ap-southeast-2", profile_name="default")
stream = kinesis.create_stream("BotoDemo", 1)
kinesis.describe_stream("BotoDemo")

kinesis.list_streams()

class Users(testdata.DictFactory):
    firstname = testdata.FakeDataFactory('firstName')
    lastname = testdata.FakeDataFactory('lastName')
    address = testdata.FakeDataFactory('address')
    age = testdata.RandomInteger(10, 30)
    gender = testdata.RandomSelection(['female', 'male'])


for user in Users().generate(100):
    print(user)
    # kinesis.put_record("BotoDemo"), json.dumps(user), "partitionkey")

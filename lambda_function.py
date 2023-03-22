import boto3
import gzip
import uuid
from datetime import datetime
from datetime import timedelta
import time
from botocore.exceptions import ClientError

#Creates a time to live value
def ttl_time():
    now = datetime.now()
    ttl_date = now + timedelta(90)
    final = str(time.mktime(ttl_date.timetuple()))
    return final

#Puts the log json into dynamodb:
def put_to_dynamo(record):
    client = boto3.resource('dynamodb', region_name='us-west-2')
    table = client.Table('YOUR_TABLE')
    try:
        response = table.put_item(
            Item=record
        )
        print(response)
    except ClientError as e:
        print("Failed to put record")
        print(e)
        return False

    return True
def lambda_handler(event, context):
    print(event)
    s3_key = event['Records'][0]['s3']['object']['key']
    s3 = boto3.resource("s3")
    obj = s3.Object("YOUR_BUCKET", s3_key)
    with gzip.GzipFile(fileobj=obj.get()["Body"]) as gzipfile:
        content = gzipfile.read()

    my_json = content.decode('utf8').splitlines()

    my_dict = {}
    for x in my_json:
        if x.startswith("#Fields:"):
            keys = x.split(" ")
        else:
            values = x.split("\t")

    for key in keys:
        if key == "#Fields:":
            pass
        else:
            for value in values:
                my_dict[key] = value
    x = 0
    for item in keys:
        if item == "#Fields:":
            pass
        else:
            my_dict[item] = values[x]
            x +=1


    print('- ' * 20)
    myuuid = str(uuid.uuid4())
    print(myuuid)
    my_dict["uuid"] = myuuid
    my_dict['ttl'] = ttl_time()

    print(my_dict)
    if put_to_dynamo(my_dict) == True:
        print("Successfully imported item")
        return True
    else:
        print("Failed to put record")
        return False
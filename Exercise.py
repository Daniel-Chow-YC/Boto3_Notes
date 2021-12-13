# Exercise: Work out how to load python/happiness-2019.csv into a usable form (i.e. a pandas dataframe)
# Load the pandas dataframe you created back into s3 into Data25/danielchow.csv
# 2 options
# 1: Load ion dataframe without saving as csv first - need to use io package to serialize data
# 2: Use the resource to load csv file into s3

import io
import pandas as pd
import boto3
import pprint as pp
import json

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

bucket_name = 'data-eng-resources'
key_name = 'python/happiness-2019.csv'

s3_object = s3_client.get_object(
    Bucket=bucket_name,
    Key=key_name
)

strbody = s3_object['Body']

happiness_df = pd.read_csv(strbody)

# 1:
csv_buffer = io.StringIO()

happiness_df.to_csv(csv_buffer, index=False)

s3_client.put_object(
    Bucket=bucket_name,
    Key='Data25/danielchow.csv',
    Body=csv_buffer.getvalue()
)

# # 2:
# # Create csv file
# happiness_df.to_csv('danielchow2.csv', index=False);
# # Upload created file
# s3_client.upload_file(Filename='danielchow2.csv', Bucket=bucket_name, Key='Data25/danielchow2.csv')
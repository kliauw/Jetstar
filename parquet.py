import pandas as pd
import numpy as np
import boto3
import io
import pyarrow as pq
import re

desired_width=320
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option('display.max_columns', 15)

#df = pd.read_csv("20190924_responsys_group_sent_v1.csv.gz")
#df = df.groupby('CAMPAIGN_ID')['CAMPAIGN_ID'].count().reset_index(name='Sent').reindex(columns=['CAMPAIGN_ID','Sent'])
#print(df)
#df.to_csv('sent_emails.csv', index=False)

# Read the parquet file
#buffer = io.BytesIO()
#s3 = boto3.resource('s3')
#object = s3.Object('jq-ada-dev-dst-adobe-parquet-test','group/v1/hit_data/part-00091-6e53ac33-e6b6-4285-b932-38168af092bf.c000.snappy.parquet')
#s3_response_object = s3.Object('jq-ada-dev-dst-adobe-parquet-test','group/v1/hit_data/part-00091-6e53ac33-e6b6-4285-b932-38168af092bf.c000.snappy.parquet')
#df = s3_response_object['Body'].read()
df = pd.read_parquet('part-00168-6e53ac33-e6b6-4285-b932-38168af092bf.c000.snappy.parquet',engine='pyarrow')
print(df)




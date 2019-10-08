import pandas as pd
import numpy as np
import boto3
import awss3fs

desired_width=320
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option('display.max_columns', 15)

s3 = boto3.resource('s3')

key = 'group/v1/sent/2019/01/01/20190101_responsys_group_sent_v1.csv.gz'
bucket = 'jq-ada-dev-dst-responsys'
location = 's3://{}/{}'.format(bucket,key)
df = pd.read_csv(location,compression='gzip', header=0, sep=',', quotechar='"')

#df = pd.read_csv("20190924_responsys_group_sent_v1.csv.gz")
df = df.groupby('CAMPAIGN_ID')['CAMPAIGN_ID'].count().reset_index(name='Sent').reindex(columns=['CAMPAIGN_ID','Sent'])
print(df)
#df.to_csv('sent_emails.csv', index=False)






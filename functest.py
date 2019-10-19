from s3fs import S3FileSystem

import pandas as pd

from pandas.io.common import EmptyDataError
s3 = S3FileSystem()
dst_bucket = "jq-ada-dev-dst-responsys"


def create_dataframes(type, location=False):
    if 'launch_state' in type:
        try:
            dframe = pd.read_csv(location, compression='gzip', dtype=str, header=0, sep=',', quotechar='"', usecols=['LAUNCH_ID','CAMPAIGN_NAME'])
            if dframe.empty != 'True':
                 dframe = dframe.groupby(['LAUNCH_ID']).sum()
                 print(dframe)
                 #dframe['LAUNCH_ID'] = dframe['LAUNCH_ID'].astype(str)
        except EmptyDataError:
            dframe = pd.DataFrame()
        dframe.to_csv('launch_state.csv', header=True)
        return dframe

    elif 'dynamic_content' in type:
        try:
            dframe = pd.read_csv(location, compression='gzip',  header=0, sep=',', quotechar='"', usecols=['LAUNCH_ID','SIGNATURE_ID','REGION_NAME','RULE_NAME'])
            if dframe.empty != 'True':
                 dframe = dframe.groupby(['LAUNCH_ID','REGION_NAME','RULE_NAME']).sum()
                 print(dframe)
                 #dframe['LAUNCH_ID'] = dframe['LAUNCH_ID'].astype(str)
            else:
                dframe = ''

        except EmptyDataError:
            dframe = pd.DataFrame()

        dframe.to_csv('dynamic_content.csv', header=True)
        return dframe

    elif 'sent' in type:
        try:
            dframe = pd.read_csv(location, compression='gzip', header=0, sep=',', quotechar='"', usecols=['LAUNCH_ID','DYNAMIC_CONTENT_SIGNATURE_ID'])
            dframe.to_csv('sent-as-is.csv', index=False, header=True)
            dframe = dframe.fillna(-1)
            # print(dframe.head(100))
            dframe = dframe.groupby(['LAUNCH_ID', 'DYNAMIC_CONTENT_SIGNATURE_ID'])['LAUNCH_ID'].count().reset_index(name=type)
            #dframe = dframe.groupby('LAUNCH_ID')['LAUNCH_ID'].count().reset_index(name=type)

            #if dframe.empty != 'True':
                #dframe = dframe.groupby(['LAUNCH_ID','DYNAMIC_CONTENT_SIGNATURE_ID'])['LAUNCH_ID'].count().reset_index(name=type).reindex(columns=['LAUNCH_ID','DYNAMIC_CONTENT_SIGNATURE_ID',type])
                #dframe = dframe.groupby(['LAUNCH_ID','DYNAMIC_CONTENT_SIGNATURE_ID'])['DYNAMIC_CONTENT_SIGNATURE_ID'].count()
                #dframe['LAUNCH_ID'] = dframe['LAUNCH_ID'].astype(str)
        except EmptyDataError:
            dframe = pd.DataFrame()

        dframe.to_csv('sent.csv', index=False, header=True)
        return dframe

    else:
        try:
            dframe = pd.read_csv(location, compression='gzip', dtype=str, header=0, sep=',', quotechar='"', usecols=['LAUNCH_ID'])
            if dframe.empty != 'True':
                dframe = dframe.groupby('LAUNCH_ID')['LAUNCH_ID'].count().reset_index(name=type).reindex(columns=['LAUNCH_ID', type])
                #dframe['LAUNCH_ID'] = dframe['LAUNCH_ID'].astype(str)
        except EmptyDataError:
            dframe = pd.DataFrame()

        print(dframe)
        return dframe

#create_dataframes('launch_state', 's3://jq-ada-dev-dst-responsys/group/v1/launch_state/2019/10/14/20191014_responsys_group_launch_state_v1.csv.gz')
#create_dataframes('sent', 's3://jq-ada-dev-dst-responsys/group/v1/sent/2019/10/14/20191014_responsys_group_sent_v1.csv.gz')
create_dataframes('dynamic_content', 's3://jq-ada-dev-dst-responsys/group/v1/dynamic_content/2019/10/13/20191013_responsys_group_dynamic_content_v1.csv.gz')
#location =  (s3.ls('/'.join([dst_bucket, "group/v1" + 'convert' + '/' + '2017' + "/" + '12'])))
#print(location)
# if location == '':
#     location = 'file not found'
# else:
#     location = 's3://' #+ location
# print(location)

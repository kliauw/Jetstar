from s3fs import S3FileSystem

import pandas as pd

from pandas.io.common import EmptyDataError
s3 = S3FileSystem()
dst_bucket = "jq-ada-dev-dst-responsys"


def create_dataframes(type, location=False):
    if location:
        try:
            dframe = pd.read_csv(location, compression='gzip', dtype=str, header=None, sep=',', quotechar='"', usecols=[1,8,50,104,286],names=['culture','campaign','revenue','product','event'])
            print(dframe)
            dframe.to_csv('adobe_hit', index=False)
            if dframe.empty != 'True':
                 dframe = dframe.groupby('campaign')['campaign'].count().reset_index(name=type).reindex(columns=['campaign', type])
                 dframe['campaign'] = dframe['campaign'].astype(str)
        except EmptyDataError:
            dframe = pd.DataFrame()

    return dframe

create_dataframes('hit_data', 's3://jq-ada-dev-dst-adobe/group/v1/hit_data/2017/01/01/00/20170101_00_adobeanalytics_group_jetstarprd_hit_data_v1.csv.gz')

#location =  (s3.ls('/'.join([dst_bucket, "group/v1" + 'convert' + '/' + '2017' + "/" + '12'])))
#print(location)
# if location == '':
#     location = 'file not found'
# else:
#     location = 's3://' #+ location
# print(location)

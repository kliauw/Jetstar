from s3fs import S3FileSystem
import re
import pandas as pd
import sys
import boto3
from pandas.io.common import EmptyDataError

def create_dataframes(location, type):

    # if 'file not found' in location:
    #     dframe = pd.DataFrame()
    # else:
    #     try:
    #         dframe = pd.read_csv(location, compression='gzip', header=0, sep=',', quotechar='"')
            # if dframe.empty != 'True':
            #     dframe = dframe.groupby('campaign')['campaign'].count().reset_index(name=type).reindex(columns=['campaign', type])
            #     dframe['campaign'] = dframe['campaign'].astype(str)
            # except EmptyDataError:
            #     dframe = pd.DataFrame()

    #dframe = pd.read_csv(location, usecols=['campaign'], compression='gzip', header=0, sep=',', quotechar='"')
    dframe = pd.read_csv(location, compression='gzip', header=0, sep=',', quotechar='"')
    print(dframe)
    return dframe

create_dataframes('s3://jq-ada-dev-dst-adobe/group/v1/hit_data/2019/10/08/15/20191008_150000_adobeanalytics_group_jetstarprd_hit_data_v1.csv.gz', 'hit_data')
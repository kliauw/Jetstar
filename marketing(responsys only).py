from s3fs import S3FileSystem
import re
import pandas as pd
from pandas.io.common import EmptyDataError
import sys
import boto3

count = 0
dst_bucket = "jq-ada-dev-dst-responsys"
s3 = S3FileSystem()
s3c = boto3.client('s3')
type_regex='(?P<bucket>[a-z-]{1,50})/group/v1/(?P<dtype>[a-z_]{1,50})'
path_regex='(?P<bucket>[a-z-]{1,50})/group/v1/(?P<dtype>[a-z_]{1,50})/(?P<Year>[0-9]{4})/(?P<Month>[0-9]{2})/(?P<Day>[0-9]{2})'
types_paths = s3.ls('/'.join([dst_bucket, "group/v1/"]))
types = [ re.search(type_regex, x).group(2) for x in types_paths]


def create_dataframes(location, type):
    if 'file not found' in location:
        dframe = pd.DataFrame()
    else:
        try:
            dframe = pd.read_csv(location, compression='gzip', header=0, sep=',', quotechar='"')
            if dframe.empty != 'True':
                dframe = dframe.groupby('CAMPAIGN_ID')['CAMPAIGN_ID'].count().reset_index(name=type).reindex(columns=['CAMPAIGN_ID', type])
                dframe['CAMPAIGN_ID'] = dframe['CAMPAIGN_ID'].astype(str)
        except EmptyDataError:
            dframe = pd.DataFrame()
    return dframe


for year in s3.ls('/'.join([dst_bucket, "group/v1/sent"])):
    for month in s3.ls(year):
        for day in s3.ls(month):
            count +=1
            Y = re.search(path_regex, day).group(3)
            m = re.search(path_regex, day).group(4)
            d = re.search(path_regex, day).group(5)
            location = ''
            location = location.join(s3.ls('/'.join([dst_bucket, "group/v1/sent/" + Y + "/" + m + "/" + d])))
            location = 's3://' + location
            combo_df = create_dataframes(location, 'sent')

            for type in types:
                count += 1
                if type not in ["sent"]:

                    new_type = day.replace("sent",type)
                    Y = re.search(path_regex, new_type).group(3)
                    m = re.search(path_regex, new_type).group(4)
                    d = re.search(path_regex, new_type).group(5)
                    type_df = type + '_df'
                    location = location.join(s3.ls('/'.join([dst_bucket, "group/v1/" + type + '/' + Y + "/" + m + "/" + d])))
                    if location == '':
                        location = 'file not found'
                    else:
                        location = 's3://' + location


                    type_df = create_dataframes(location, type)
                    if not type_df.empty:
                        combo_df = pd.merge(combo_df, type_df, on='CAMPAIGN_ID', how='outer')

                    print(combo_df)
                    if count > 10:
                        sys.exit()





                    file_string = Y+m+d + '_responsys_marketing.csv.gz'
                    combo_df.to_csv(file_string, index=False, compression='gzip')
                    s3c.put_object(Bucket='test-jq-ada-dev-marketing', Key=('responsys/'))
                    s3c.upload_file(file_string, 'test-jq-ada-dev-marketing', 'responsys/'+file_string)






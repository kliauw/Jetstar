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

print(types_paths)
types = [ re.search(type_regex, x).group(2) for x in types_paths]


def create_dataframes(location, type):

    try:
        dframe = pd.read_csv(location, compression='gzip', header=0, sep=',', quotechar='"')
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
            print("\nAdd this to dataframe: sent;" + Y + ";" + m + ";" + d)
            location = ''
            location = location.join(s3.ls('/'.join([dst_bucket, "group/v1/sent/" + Y + "/" + m + "/" + d])))
            location = 's3://' + location
            #print(location)

            combo_df = create_dataframes(location, 'sent')
            #print(combo_df)


            for type in types:
                count += 1
                if type not in ["sent","complaint","convert","customer","fail","form","form_state","program","program_state","skipped"]:

                    new_type = day.replace("sent",type)
                    Y = re.search(path_regex, new_type).group(3)
                    m = re.search(path_regex, new_type).group(4)
                    d = re.search(path_regex, new_type).group(5)
                    print("Add this to dataframe: " + type + ";" + Y + ";" + m + ";" + d)
                    type_df = type + '_df'
                    print('TYPE:'+type)
                    location = ''
                    location = location.join(s3.ls('/'.join([dst_bucket, "group/v1/" + type + '/' + Y + "/" + m + "/" + d])))
                    location = 's3://' + location
                    last_slash = location.rfind('/')


                    type_df = create_dataframes(location, type)
                    if type_df.empty != 'True':
                        combo_df = pd.merge(combo_df, type_df, on='CAMPAIGN_ID', how='outer')
                    #print(combo_df)
                    if count > 33:
                        sys.exit()
                    file_string = Y+m+d + '_responsys_marketing.csv.gz'
                    combo_df.to_csv(file_string, index=False, compression='gzip')
                    s3c.put_object(Bucket='test-jq-ada-dev-marketing', Key=('responsys/'))
                    s3c.upload_file(file_string, 'test-jq-ada-dev-marketing', 'responsys/'+file_string)






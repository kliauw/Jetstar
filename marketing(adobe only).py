from s3fs import S3FileSystem
import re
import pandas as pd
from pandas.io.common import EmptyDataError
import sys
import boto3

count = 0
dst_bucket = "jq-ada-dev-dst-adobe"
s3 = S3FileSystem()
s3c = boto3.client('s3')
type_regex='(?P<bucket>[a-z-]{1,50})/group/v1/(?P<dtype>[a-z_]{1,50})'
path_regex='(?P<bucket>[a-z-]{1,50})/group/v1/(?P<dtype>[a-z_]{1,50})/(?P<Year>[0-9]{4})/(?P<Month>[0-9]{2})/(?P<Day>[0-9]{2})/(?P<Hour>[0-9]{2})'
#path_regex='(?P<bucket>[a-z-]{1,50})/group/v1/(?P<dtype>[a-z_]{1,50})/(?P<Year>[0-9]{4})/(?P<Month>[0-9]{2})/(?P<Day>[0-9]{2})/(?P<Hour>[0-9]{2})/(?P<YearMD>[0-9]{6})_(?P<Time>[0-9]{2,6})'

types_paths = s3.ls('/'.join([dst_bucket, "group/v1/"]))
types = [ re.search(type_regex, x).group(2) for x in types_paths]

# print(types_paths)
# print(types)
def create_dataframes(location, type):
    if 'file not found' in location:
        dframe = pd.DataFrame()
    else:
        try:
            dframe = pd.read_csv(location, compression='gzip', header=0, sep=',', quotechar='"')
            # if dframe.empty != 'True':
            #     dframe = dframe.groupby('CAMPAIGN_ID')['CAMPAIGN_ID'].count().reset_index(name=type).reindex(columns=['CAMPAIGN_ID', type])
            #     dframe['CAMPAIGN_ID'] = dframe['CAMPAIGN_ID'].astype(str)
        except EmptyDataError:
            dframe = pd.DataFrame()
    return dframe

x=''

for year in s3.ls('/'.join([dst_bucket, "group/v1/hit_data"])):
    if x in year:
     #print(year)
        for month in s3.ls(year):
            #print(month)
            for day in s3.ls(month):
                #print(day)
                for hour in s3.ls(day):
                    #print(hour)
                    for csv_file in s3.ls(hour):
                        print("csv xxxxxx"+csv_file)
                        print("hour xxxxxx" + hour)
                        count +=1
                        Y = re.search(path_regex, hour).group(3)
                        m = re.search(path_regex, hour).group(4)
                        d = re.search(path_regex, hour).group(5)
                        h = re.search(path_regex, hour).group(6)

                        mystring = csv_file.replace(hour,'')
                        mystring = mystring.split("_")
                        t = mystring[1]
                        if 'mobileapp' in csv_file:
                            app = Y + m+ d + '_' + t + '_adobeanalytics_group_jetstarmobileappprd_hit_data_v1.csv.gz'
                        else:
                            app = Y + m+ d + '_' + t + '_adobeanalytics_group_jetstarprd_hit_data_v1.csv.gz'
                        print("app var"+app)
                        location = ''
                        #location = location.join(s3.ls('/'.join([dst_bucket, "group/v1/hit_data/" + Y + "/" + m + "/" + d + "/" + h + "/"])))
                        location = dst_bucket + "/group/v1/hit_data/" + Y + "/" + m + "/" + d + "/" + h + "/" + app
                        #location = s3.ls('/'.join([dst_bucket, "group/v1/hit_data/" + Y + "/" + m + "/" + d + "/" + h]))
                        location = 's3://' + location
                        print(location)
                        #combo_df = create_dataframes(location, 'hit_data')
                        #dframe = pd.read_csv(location, compression='gzip', header=0, sep=',', quotechar='"')

                        #print(combo_df)

                # for type in types:
                #     count += 1
                #     if type not in ["sent"]:
                #
                #         new_type = day.replace("sent",type)
                #         Y = re.search(path_regex, new_type).group(3)
                #         m = re.search(path_regex, new_type).group(4)
                #         d = re.search(path_regex, new_type).group(5)
                #         type_df = type + '_df'
                #         location = location.join(s3.ls('/'.join([dst_bucket, "group/v1/" + type + '/' + Y + "/" + m + "/" + d])))
                #         if location == '':
                #             location = 'file not found'
                #         else:
                #             location = 's3://' + location
                #

                        # type_df = create_dataframes(location, type)
                        # if not type_df.empty:
                        #     combo_df = pd.merge(combo_df, type_df, on='CAMPAIGN_ID', how='outer')
                        #
                        # print(combo_df)
                        # if count > 10:
                        #     sys.exit()
                        #
                        #
                        #
                        #
                        #
                        # file_string = Y+m+d + '_responsys_marketing.csv.gz'
                        # combo_df.to_csv(file_string, index=False, compression='gzip')
                        # s3c.put_object(Bucket='test-jq-ada-dev-marketing', Key=('responsys/'))
                        # s3c.upload_file(file_string, 'test-jq-ada-dev-marketing', 'responsys/'+file_string)

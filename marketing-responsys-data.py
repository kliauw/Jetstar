from s3fs import S3FileSystem
import re
import pandas as pd
from pandas.io.common import EmptyDataError



dst_bucket = "jq-ada-dev-dst-responsys"
s3 = S3FileSystem()
type_regex='(?P<bucket>[a-z-]{1,50})/group/v1/(?P<dtype>[a-z_]{1,50})'
path_regex='(?P<bucket>[a-z-]{1,50})/group/v1/(?P<dtype>[a-z_]{1,50})/(?P<Year>[0-9]{4})/(?P<Month>[0-9]{2})/(?P<Day>[0-9]{2})'
types_paths = s3.ls('/'.join([dst_bucket, "group/v1/"]))
types = [ re.search(type_regex, x).group(2) for x in types_paths]


def create_dataframes(type, location=False):
    if location:
        try:
            dframe = pd.read_csv(location, compression='gzip', dtype=str, header=0, sep=',', quotechar='"', usecols=['CAMPAIGN_ID'])
            if dframe.empty != 'True':
                dframe = dframe.groupby('CAMPAIGN_ID')['CAMPAIGN_ID'].count().reset_index(name=type).reindex(columns=['CAMPAIGN_ID', type])
                dframe['CAMPAIGN_ID'] = dframe['CAMPAIGN_ID'].astype(str)
        except EmptyDataError:
            dframe = pd.DataFrame()
    return dframe


for year in s3.ls('/'.join([dst_bucket, "group/v1/sent"])):
#if '2017' in year:
    for month in s3.ls(year):
        #if '04' in month:
            for day in s3.ls(month):
                Y = re.search(path_regex, day).group(3)
                m = re.search(path_regex, day).group(4)
                d = re.search(path_regex, day).group(5)
                location = s3.ls('/'.join([dst_bucket, "group/v1/sent", Y, m, d]))
                new_location = '/'.join(['test-jq-ada-dev-marketing', "group/v1/responsys", Y, m, d])
                #if not s3.ls(new_location):
                if 1==1:
                    location = 's3://' + location[0]
                    combo_df = create_dataframes('sent', location)

                    for type in types:
                        try:
                            if type not in ["sent","customer","program","program_state"]:
                                new_type = day.replace("sent",type)
                                Y = re.search(path_regex, new_type).group(3)
                                m = re.search(path_regex, new_type).group(4)
                                d = re.search(path_regex, new_type).group(5)
                                type_df = type + '_df'
                                location = s3.ls('/'.join([dst_bucket, "group/v1", type, Y, m, d]))
                                location = 's3://' + location[0]

                                type_df = create_dataframes(type,location)
                                if not type_df.empty:
                                    combo_df = pd.merge(combo_df, type_df, on='CAMPAIGN_ID', how='outer')
                                    print(combo_df)
                                else:
                                    combo_df[type] = ''


                        except Exception:
                            # Add blank column
                            combo_df[type]=''


                        file_string = Y+m+d + '_responsys_marketing.csv.gz'
                        combo_df.to_csv(file_string, index=False, compression='gzip')
                        #s3.put(file_string, '/'.join(['jq-ada-dev-marketing-test/group/v1/responsys', Y, m, d, file_string]))


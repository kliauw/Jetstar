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


# def create_dataframes(type, location=False):
#     if location:
#         try:
#             dframe = pd.read_csv(location, compression='gzip', dtype=str, header=0, sep=',', quotechar='"', usecols=['LAUNCH_ID'])
#             if dframe.empty != 'True':
#                 dframe = dframe.groupby('LAUNCH_ID')['LAUNCH_ID'].count().reset_index(name=type).reindex(columns=['LAUNCH_ID', type])
#                 dframe['LAUNCH_ID'] = dframe['LAUNCH_ID'].astype(str)
#         except EmptyDataError:
#             dframe = pd.DataFrame()
#     return dframe

def create_dataframes(type, location=False):
    if 'launch_state' in type:
        try:
            dframe = pd.read_csv(location, compression='gzip', dtype=str, header=0, sep=',', quotechar='"', usecols=['LAUNCH_ID','CAMPAIGN_NAME'])
            if dframe.empty != 'True':
                dframe = dframe.drop_duplicates()
                #dframe['LAUNCH_ID'] = dframe['LAUNCH_ID'].astype(str)
        except EmptyDataError:
            dframe = pd.DataFrame()
        #print(dframe)
        return dframe

    elif 'dynamic_content' in type:
        try:
            dframe = pd.read_csv(location, compression='gzip', dtype=str, header=0, sep=',', quotechar='"', usecols=['LAUNCH_ID','SIGNATURE_ID','REGION_NAME','RULE_NAME'])
            if dframe.empty != 'True':
                dframe = dframe.drop_duplicates()
                dframe['SIGNATURE_ID'] = dframe['SIGNATURE_ID'].astype(str)
        except EmptyDataError:
            dframe = pd.DataFrame()
        print(dframe)
        return dframe

    elif 'sent' in type:
        try:
            dframe = pd.read_csv(location, compression='gzip',
                                 header=0,
                                 dtype=str,
                                 sep=',', quotechar='"', usecols=['LAUNCH_ID', 'DYNAMIC_CONTENT_SIGNATURE_ID'])
            dframe = dframe.fillna(-1)

            if dframe.empty != 'True':
                # print(dframe.head(100))
                dframe = dframe.groupby(['LAUNCH_ID', 'DYNAMIC_CONTENT_SIGNATURE_ID'])['LAUNCH_ID'].count().reset_index(
                    name=type)
                # dframe['DYNAMIC_CONTENT_SIGNATURE_ID'] = dframe['DYNAMIC_CONTENT_SIGNATURE_ID'].astype(str)
        except EmptyDataError:
            dframe = pd.DataFrame()
        return dframe

    else:
        try:
            dframe = pd.read_csv(location, compression='gzip', dtype=str, header=0, sep=',', quotechar='"', usecols=['LAUNCH_ID'])
            if dframe.empty != 'True':
                dframe = dframe.groupby('LAUNCH_ID')['LAUNCH_ID'].count().reset_index(name=type).reindex(columns=['LAUNCH_ID', type])
                #dframe['LAUNCH_ID'] = dframe['LAUNCH_ID'].astype(str)
        except EmptyDataError:
            dframe = pd.DataFrame()

        return dframe

for year in s3.ls('/'.join([dst_bucket, "group/v1/sent"])):
    if '2019' in year:
        print('in yr')
        for month in s3.ls(year):
            print('in mth')
            if '10' in month:
                for day in s3.ls(month):
                    print('in day')
                    if '14' in day:
                        Y = re.search(path_regex, day).group(3)
                        m = re.search(path_regex, day).group(4)
                        d = re.search(path_regex, day).group(5)
                        print(d)
                        location = s3.ls('/'.join([dst_bucket, "group/v1/sent", Y, m, d]))
                        new_location = '/'.join(['jq-ada-dev-marketing-test', "group/v1/responsys", Y, m, d])
                        #if not s3.ls(new_location):
                        if 1 == 1:
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
                                        print(type)
                                        print(location)

                                        if type not in ['launch_state','dynamic_content']:

                                            type_df = create_dataframes(type,location)
                                            if not type_df.empty:
                                                combo_df = pd.merge(combo_df, type_df, on='LAUNCH_ID', how='outer')
                                                combo_df.to_csv('combo_df.csv', index=False)
                                            else:
                                                combo_df[type] = ''

                                        elif 'dynamic_content' in type:
                                            print('Hello')
                                            dynamic_file_string = Y + m + d + '_dynamic_content.csv.gz'
                                            dynamic_content_df = create_dataframes(type, location)
                                            #print(dynamic_content_df)
                                            dynamic_content_df.to_csv('dynamic_content.csv', index=False)
                                            #dynamic_content_df.to_csv(dynamic_file_string, index=False, compression='gzip')
                                        elif 'launch_state' in type:
                                            print('In launch')
                                            launch_file_string = Y + m + d + '_launch_state.csv'
                                            launch_state_df = create_dataframes(type, location)
                                            #print(launch_state_df)
                                            launch_state_df.to_csv(launch_file_string, index=False)

                                        #elif 'dynamic_content' in type:
                                        # else:
                                        #     print('Hello')
                                        #     dynamic_file_string = Y + m + d + '_dynamic_content.csv.gz'
                                        #     dynamic_content_df = create_dataframes(type, location)
                                        #     #print(dynamic_content_df)
                                        #     dynamic_content_df.to_csv('dynamic_content.csv', index=False)
                                        #     #dynamic_content_df.to_csv(dynamic_file_string, index=False, compression='gzip')

                                except Exception:
                                    # Add blank column
                                    combo_df[type] = ''


                            combo_dynamic_df = combo_df.merge(dynamic_content_df, left_on=['LAUNCH_ID','DYNAMIC_CONTENT_SIGNATURE_ID'], right_on=['LAUNCH_ID','SIGNATURE_ID'], how='left').drop('SIGNATURE_ID', axis=1)
                            print(combo_dynamic_df)
                            combo_dynamic_df.to_csv('combo_dynamic_content.csv', index=False)

                            combo_dynamic_launch_df = combo_dynamic_df.merge(launch_state_df, on=['LAUNCH_ID'])
                            print(combo_dynamic_launch_df)
                            combo_dynamic_launch_df.to_csv('combo_dynamic_launch_df.csv', index=False)


                            #combo_dynamic_launch_df = combo_dynamic_df.merge(launch_state_df, on='LAUNCH_ID', how='left')
                            #print(combo_dynamic_launch_df)
                            #combo_file_string = Y+m+d + '_responsys_marketing.csv.gz'
                            #combo_dynamic_launch_df.to_csv(combo_file_string, index=False, compression='gzip')

                            #s3.put(file_string, '/'.join(['jq-ada-dev-marketing-test/group/v1/responsys', Y, m, d, file_string]))


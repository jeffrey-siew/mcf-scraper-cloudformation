import pandas as pd
import requests
import datetime
import numpy as np
import awswrangler as wr
import os    

def get_handles_new_link(splitted_list: list):
    """
    The function that would run in multi-threaded function 'setup_threaded_workers_search'
    """
    output_scrapped_df = pd.DataFrame()
    for each_page in splitted_list:
        print(each_page)
        try:
            url = ('https://api.mycareersfuture.gov.sg/v2/jobs?limit=20&page=' + str(each_page) + '&sortBy=new_posting_date')
            json_temp = requests.get(url).json()
            temp_scrapped_df = collect_data_json(json_temp)
            frames = [output_scrapped_df, temp_scrapped_df]
            output_scrapped_df = pd.concat(frames)
        except:
            print('error in downloading or concat data')
    s3_save_instance_df(output_scrapped_df)


def s3_save_instance_df(input_df: pd.DataFrame):
    """
    aws s3 saving of scrapped data base on individual run time
    this approach would create single csv file every day or append to existing file
    """
    # setting the filler for the naming convention
    now = datetime.datetime.now() + datetime.timedelta(hours=8)
    # setting the S3 bucket name
    aws_s3_bucket = 'mcf'
    
    # setting the S3 object name / file name
    key = f'temp/split_3'
    
    wr.s3.to_parquet(
        df=input_df,
        path=f"s3://{aws_s3_bucket}/{key}",
        dataset=True,
        mode="overwrite")
    
    # uploading of csv directly to s3 via pandas, achieved via usage of s3fs
    #input_df.to_csv(
    #    f"s3://{aws_s3_bucket}/{key}",
    #    index=False,
    #)
    print(f'File {key} saved')

def collect_data_json(input_json: str) -> pd.DataFrame:
    """
    This function would scrape the website from the json data and output it in a DataFrame format.
    """
    temp_json = input_json['results']
    output_df = pd.json_normalize(temp_json)

    return output_df

def lambda_handler(event, context):
    
    #print(event)
    #print(event['split_1'])
    #print(event['value2'])
    
    #url = 'https://api.mycareersfuture.gov.sg/v2/jobs?limit=20&page=0&sortBy=min_monthly_salary'
    #result_temp = requests.get(url).json()
    #web_total_page = result_temp['total']//20
    
    #print(web_total_page)
    
    #web_total_page_list = []
    #for each in range(web_total_page):
    #    web_total_page_list.append(each)
    
    #print(web_total_page_list[-1])
    
    #partitions = 3
    #splitted_list = np.array_split(web_total_page_list,partitions)
    
    #splitted_list = event['split_3']
    
    #get_handles_new_link(splitted_list)

    start = event['split_3_st']
    end = event['split_3_en']
    
    web_total_page_list = []
    for each in range(int(start),int(end)+1):
        web_total_page_list.append(each)
    
    print(web_total_page_list)
    
    get_handles_new_link(web_total_page_list)

    #setup_threaded_workers_search(web_total_page_list)
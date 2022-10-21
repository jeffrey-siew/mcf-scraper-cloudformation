import pandas as pd
import awswrangler as wr
import datetime

def lambda_handler(event, context):
    """_summary_

    Args:
        event (_type_): _description_
        context (_type_): _description_

    Returns:
        json: _description_
    """    
    # load up jobid_list for jobs that was previously scraped
    jobid_list = s3_load_jobid_list()
    print(len(jobid_list))
    # loading up temp data that was scraped
    temp_df = s3_load_temp_df()
    print(temp_df.shape)
    # transform df for new meta data
    temp_meta_df = get_new_meta_data(temp_df)
    # save new meta data
    s3_save_meta_df(temp_meta_df)
    # 
    new_job_df = get_new_job_data(temp_df, jobid_list)
    print(new_job_df.shape)
    # 
    s3_save_jobid(new_job_df)
    # 
    s3_key = s3_save_instance_df(new_job_df)

    return {
        'Payload': {
            's3_key':s3_key
                }
            }
    
def s3_load_temp_df() -> pd.DataFrame:
    """_summary_

    Returns:
        pd.DataFrame: _description_
    """    
    aws_s3_bucket = 'mcf/temp'
    
    key_list = [
        'split_2',
        'split_3',
        'split_4',
        'split_5'
        ]
    
    #output_df = pd.DataFrame
    output_df = wr.s3.read_parquet(f"s3://{aws_s3_bucket}/split_1")
    for key in key_list:
        
        temp_df = wr.s3.read_parquet(f"s3://{aws_s3_bucket}/{key}")
        
        #temp_df = pd.read_parquet(
        #        f"s3://{aws_s3_bucket}/{key}",
        #    engine='pyarrow', 
        #    compression='snappy',
        #    )
        
        frames = [output_df, temp_df]
        # Combining the website_new and website_old dataframe into one dataframe 
        output_df = pd.concat(
                            frames,
                            axis=0,
                            join="outer",
                            ignore_index=True,
                            keys=None,
                            levels=None,
                            names=None,
                            verify_integrity=False
                        )
        
    return output_df
    
def s3_save_instance_df(input_df: pd.DataFrame) -> str:
    """aws s3 saving of scrapped data base on individual run time
    this approach would create single csv file every day or append to existing file

    Args:
        input_df (pd.DataFrame): _description_

    Returns:
        str: s3_key
    """
    # setting the filler for the naming convention
    now = datetime.datetime.now() + datetime.timedelta(hours=8)
    year = now.year
    month = '%02d' % now.month
    day = '%02d' % now.day
    
    # setting the S3 bucket name
    aws_s3_bucket = 'mcf'
    
    # setting the S3 object name / file name
    s3_key = f'raw_data/{year}/{month}/{day}'

    try:
        # reading of csv directly from s3 via awsrangler
        exist_df = wr.s3.read_parquet(f"s3://{aws_s3_bucket}/{s3_key}")
        # reading of csv directly from s3 via pandas, achieved via usage of s3fs
        #exist_df = pd.read_csv(
        #    f"s3://{aws_s3_bucket}/{key}",
        #)
        # Setting the list of dataframe
        frames = [exist_df, input_df]
        # Combining the website_new and website_old dataframe into one dataframe 
        input_df = pd.concat(
                            frames,
                            axis=0,
                            join="outer",
                            ignore_index=True,
                            keys=None,
                            levels=None,
                            names=None,
                            verify_integrity=False
                        )
        print(f'Per Instance File Found, updating {s3_key} into S3')
    except:
        print(f'Per Instance File Not Found, saving new file into S3 as {s3_key}')
    
    # saving of parquet directly to s3 via awswrangler
    wr.s3.to_parquet(
        df=input_df,
        path=f"s3://{aws_s3_bucket}/{s3_key}",
        dataset=True,
        mode="overwrite")
    
    return s3_key

def s3_load_jobid_list() -> list:
    """
    aws s3 loading of website link csv file in the s3 bucket
    """
    # importing the full scrapped data from s3 bucket
    # setting the S3 bucket name
    aws_s3_bucket = 'mcf'
    
    # setting the S3 object name / file name
    key = 'joblist'

    try:
        # reading of csv directly from s3 via pandas, achieved via usage of s3fs
        #output_df = pd.read_parquet(
        #    f"s3://{aws_s3_bucket}/{key}",
        #engine='pyarrow', 
        #compression='snappy',
        #)
        #output_list = list(output_df['metadata.jobPostId'])
        
        output_df = wr.s3.read_parquet(f"s3://{aws_s3_bucket}/{key}")
        output_list = list(output_df['metadata.jobPostId'])
        
    except FileNotFoundError:
        output_list = []
        print('JobID File Not Found')

    return output_list

def get_new_job_data(input: pd.DataFrame, jobid: list) -> pd.DataFrame:
    """
    
    """
    print(len(jobid))
    print(jobid[0])
    print(jobid[-1])
    print(input.shape)
    output_scrapped_df = input[~input['metadata.jobPostId'].isin(jobid)]
    return output_scrapped_df

def s3_save_jobid(input_df: pd.DataFrame):
    """aws s3 saving of website link that was scrapped
    this would continous update the csv file to store all website link that was scrapped
    these website link form as a core reference to identify scrapped or new links

    Args:
        input_df (pd.DataFrame): _description_
    """
    # filter out the website link from scrapped data
    input_jobid_df = input_df[['metadata.jobPostId']]

    # importing the full scrapped data from s3 bucket
    # setting the S3 bucket name
    aws_s3_bucket = 'mcf'
    # setting the S3 object name / file name
    key = 'joblist'

    try:
        exist_df = wr.s3.read_parquet(f"s3://{aws_s3_bucket}/{key}")
        
        print('JobID File Found, the existing JobID file will be updated')
    except:
        exist_df = pd.DataFrame
        print('JobID File Not Found, a new JobID file will be saved')

    frames = [exist_df, input_jobid_df]
    # Combining the website_new and website_old dataframe into one dataframe 
    output_df = pd.concat(
                        frames,
                        axis=0,
                        join="outer",
                        ignore_index=True,
                        keys=None,
                        levels=None,
                        names=None,
                        verify_integrity=False
                    )
    
    # saving of parquet directly to s3 via awswrangler
    wr.s3.to_parquet(
        df=output_df,
        path=f"s3://{aws_s3_bucket}/{key}",
        dataset=True,
        mode="overwrite")

def get_new_meta_data(input_df: pd.DataFrame) -> pd.DataFrame:
    """function to filter out the metadata column from the scraped data

    Args:
        input_df (pd.DataFrame): dataframe intended for filtering for meta data

    Returns:
        pd.DataFrame: output the meta data
    """    
    output_meta_df = input_df[[
        'metadata.jobPostId','metadata.deletedAt','metadata.createdBy',
        'metadata.createdAt','metadata.updatedAt','metadata.editCount',
        'metadata.repostCount','metadata.totalNumberOfView','metadata.newPostingDate',
        'metadata.originalPostingDate','metadata.expiryDate',
        'metadata.totalNumberJobApplication','metadata.isPostedOnBehalf',
        'metadata.isHideSalary','metadata.isHideCompanyAddress',
        'metadata.isHideHiringEmployerName','metadata.isHideEmployerName',
        'metadata.jobDetailsUrl','metadata.matchedSkillsScore'
    ]]
    return output_meta_df


def s3_save_meta_df(input_df: pd.DataFrame):
    """aws s3 saving of scrapped meta data base on individual run time
    this approach would create single csv file every day

    Args:
        input_df (pd.DataFrame): the dataframe that intended to be save into the s3 bucket
    """
    # setting the filler for the naming convention
    now = datetime.datetime.now() + datetime.timedelta(hours=8)
    year = now.year
    month = '%02d' % now.month
    day = '%02d' % now.day
    
    # setting the S3 bucket name
    s3_bucket = 'mcf'
    
    # setting the S3 object name / file name
    s3_key = f'raw_meta/{year}/{month}/{day}'

    # try to check if S3 have existing file with the same key naming convention, if so load the csv and concat it with input_df before saving back to S3
    # if except FileNotFoundError is raise, it would save as per key naming convention.
    try:
        # reading of csv directly from s3 via awsrangler
        exist_df = wr.s3.read_parquet(f"s3://{s3_bucket}/{s3_key}")

        # Setting the list of dataframe
        frames = [exist_df, input_df]
        # Combining the website_new and website_old dataframe into one dataframe 
        input_df = pd.concat(
                            frames,
                            axis=0,
                            join="outer",
                            ignore_index=True,
                            keys=None,
                            levels=None,
                            names=None,
                            verify_integrity=False
                        )
        print(f'Meta File Found, Updating file {s3_key} into S3')
    except:
        print(f'Meta File Not Found, Saving new file into S3 as {s3_key}')
    
    # saving of parquet directly to s3 via awswrangler
    wr.s3.to_parquet(
        df=input_df,
        path=f"s3://{s3_bucket}/{s3_key}",
        dataset=True,
        mode="overwrite")
import requests
import numpy as np

def lambda_handler(event, context):
    """
    This function is use to generate the list of number of new job page, and the return list would be use in threadpoolexecutor
    """
    url = 'https://api.mycareersfuture.gov.sg/v2/jobs?limit=20&page=0&sortBy=min_monthly_salary'
    result_temp = requests.get(url).json()
    web_total_page = result_temp['total']//20

    web_total_page_list = []
    for each in range(web_total_page):
        web_total_page_list.append(each)
    
    partitions = 5
    splitted_list = np.array_split(web_total_page_list,partitions)
    
    return {
        'Payload': {
            'split_1_st':f'{splitted_list[0][0]}',
            'split_2_st':f'{splitted_list[1][0]}',
            'split_3_st':f'{splitted_list[2][0]}',
            'split_4_st':f'{splitted_list[3][0]}',
            'split_5_st':f'{splitted_list[4][0]}',
            'split_1_en':f'{splitted_list[0][-1]}',
            'split_2_en':f'{splitted_list[1][-1]}',
            'split_3_en':f'{splitted_list[2][-1]}',
            'split_4_en':f'{splitted_list[3][-1]}',
            'split_5_en':f'{splitted_list[4][-1]}'
            }
        }
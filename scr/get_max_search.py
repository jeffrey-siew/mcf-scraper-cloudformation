import requests
import numpy as np

def lambda_handler(event, context):
    """
    This function is use to generate the list of number of new job page, and the return list would be use in threadpoolexecutor
    """
    url = 'https://api.mycareersfuture.gov.sg/v2/jobs?limit=20&page=0&sortBy=new_posting_date'
    result_temp = requests.get(url).json()
    web_total_page = result_temp['total']//20

    web_total_page_list = []
    for each in range(web_total_page):
        web_total_page_list.append(each)
    
    partitions = 5
    splitted_list = np.array_split(web_total_page_list,partitions)
    
    return {
        'Payload': {
            'Load_1': {
                's3_key':'split_1',
                'start':f'{splitted_list[0][0]}',
                'end':f'{splitted_list[0][-1]}'
                },
            'Load_2': {
                's3_key':'split_2',
                'start':f'{splitted_list[1][0]}',
                'end':f'{splitted_list[1][-1]}'
                },
            'Load_3': {
                's3_key':'split_3',
                'start':f'{splitted_list[2][0]}',
                'end':f'{splitted_list[2][-1]}'
                },
            'Load_4': {
                's3_key':'split_4',
                'start':f'{splitted_list[3][0]}',
                'end':f'{splitted_list[3][-1]}'
                },
            'Load_5': {
                's3_key':'split_5',
                'start':f'{splitted_list[4][0]}',
                'end':f'{splitted_list[4][-1]}'
                }
            }
        }
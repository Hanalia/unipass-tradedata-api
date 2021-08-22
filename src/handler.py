import sys
sys.path.insert(0, 'src/vendor')
# sys.path.insert(0, 'tmp')

import json
import time
import asyncio
import csv
from datetime import datetime
import json
import aiohttp
from io import BytesIO
import boto3
s3_client = boto3.client('s3')

async def fetch(session, month, hscode, areakey):
    u = 'https://unipass.customs.go.kr/ets/hmpg/retrieveTkofDdBasePrlstPrImexAcrsSggLst.do'
    headers = {
        'Connection': 'keep-alive',
        'sec-ch-ua': '^\\^Chromium^\\^;v=^\\^92^\\^, ^\\^',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua-mobile': '?0',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36',
        'isAjax': 'true',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Origin': 'https://unipass.customs.go.kr',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Dest': 'empty',
        'Referer': 'https://unipass.customs.go.kr/ets/',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
    }
    async with session.post(u, headers=headers, data=get_url_data(month, hscode, areakey)) as response:
        json_response = await response.json()
        return json_response

# the multiple request, get list of [response, mymonth, area]


def get_monthlist(dates):
    start, end = [datetime.strptime(_, "%Y%m") for _ in dates]
    total_months = lambda dt: dt.month + 12 * dt.year
    mlist = []
    for tot_m in range(total_months(start)-1, total_months(end)):
        y, m = divmod(tot_m, 12)
        mlist.append(datetime(y, m+1, 1).strftime("%Y%m"))
    return mlist
def get_url_data(month, hscode, areakey):
    data = {
        'isAll': 'NO',
        'pageIndex': '1',
        'pageUnit': '10',
        'priodKind': 'MON',
        'priodFr': month,
        'priodTo': month,
        'sidoCd': areakey[0:2],
        'frdgPsno': areakey[2:],
        'hsSgn': hscode,
        #   'subStrCount': '6',
        'statsBaseTpcd': '1',
        #   'srchCondTtle': '^%^ED^%^86^%^B5^%^EA^%^B3^%^84^%^EA^%^B8^%^B0^%^EC^%^A4^%^80^%^3A^%^EC^%^B6^%^9C^%^ED^%^95^%^AD^%^EC^%^9D^%^BC^%^2C ^%^EC^%^A1^%^B0^%^ED^%^9A^%^8C^%^EA^%^B8^%^B0^%^EA^%^B0^%^84^%^3A202107 ~202107 ^%^2C ^%^EA^%^B4^%^91^%^EC^%^97^%^AD^%^EC^%^8B^%^9C(^%^EB^%^8F^%^84)^%^3A^%^EA^%^B2^%^BD^%^EA^%^B8^%^B0^%^EB^%^8F^%^84^%^2C ^%^EC^%^8B^%^9C^%^EA^%^B5^%^B0^%^EA^%^B5^%^AC^%^3A',
        #   'menuId': 'ETS_MNU_00000164',
        'orderColumns': ''
    }
    return data




async def main_async(startmonth, endmonth, hscode):

    dates = [startmonth, endmonth]
    monthlist = get_monthlist(dates)

    bucket_name = 'unipass-data'
    s3_file_name = 'newcodes.csv'
    resp = s3_client.get_object(Bucket=bucket_name, Key=s3_file_name)
    csvcontent = resp['Body'].read().decode('utf-8-sig').splitlines()
    reader = csv.reader(csvcontent)
    codelist = {}
    for row in reader:
        k, v = row
        codelist[k] = v
    my_conn = aiohttp.TCPConnector(limit=15)
    async with aiohttp.ClientSession(connector=my_conn) as session:
        tasks = []
        props = []
        for mymonth in monthlist:
            for key, area in codelist.items():
                mytask = fetch(session, mymonth, hscode, key)
                tasks.append(mytask)
                props.append([mymonth, area])
        finallist = await asyncio.gather(*tasks)
        return [finallist, props]


# the processing stage : get the list of responses and merge


# 2단계 : use list comprehension to preprocess each element in the list

# input is [[response_1, response_2 .. ], [[month1,area1], [month2,area2] .. ]]
def get_json(async_list):
    dfs = []
    # df = pd.read_csv('mycodes.csv')
    # codelist = df.set_index('CODE').T.to_dict('records')[0]  # my dict
    for response, props in zip(async_list[0], async_list[1]):

        # if no value = > put empty dataframe

        if response['count'] == 0:
            df1 = {}
            df1['date'] = props[0][0:4] + '-' + props[0][4:] + '-01'
            df1['area'] = props[1]
            df1['expCnt'] = '0'
            df1['expUsdAmt'] = '0'
            df1['impCnt'] = '0'
            df1['impUsdAmt'] = '0'
            dfs.append(df1)
        else:
            df1 = {}
            df1['date'] = props[0][0:4] + '-' + props[0][4:] + '-01'
            df1['area'] = props[1]
            df1['expCnt'] = response['items'][1]['expCnt']
            df1['expUsdAmt'] = response['items'][1]['expUsdAmt']
            df1['impCnt'] = response['items'][1]['impCnt']
            df1['impUsdAmt'] = response['items'][1]['impUsdAmt']
            dfs.append(df1)

    #  this returns json value        
    final_json = json.dumps(dfs)
    return final_json

    # ## this saves as json
    # with open("data.json", "w") as fp:
    #     json.dump(dfs , fp) 





# start = time.time()
# mylist = asyncio.run(main_async('202107', '202107', '382200'))
# myresult = get_json(mylist)
# get_json(mylist)

# end = time.time()
# print(end - start)






def getdata(event, context):
    mylist = asyncio.run(main_async('202107', '202107', '382200'))
    myresult = get_json(mylist)

    response = {
        "statusCode": 200,
        "body": myresult
    }

    return response

    # Use this code if you don't use the http event with the LAMBDA-PROXY
    # integration
    """
    return {
        "message": "Go Serverless v1.0! Your function executed successfully!",
        "event": event
    }
    """

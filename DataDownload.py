import requests
import json
import datetime
import pandas as pd
import os
from tqdm import tqdm

def transform(x):
    # 可以用来转换时间
    return datetime.datetime.fromtimestamp(int(x) // 1000)


def get_history(names, max_time, begin_time=None):
    if begin_time is None:
        begin_time = 1621210920000
    for name in names:
        data_all = []
        if begin_time is not None:
            request_link = f"https://www.okex.com/api/v5/market/history-candles?instId=" + name + '&after=' + str(begin_time)
        print("Request link: " + request_link)
        r = requests.get(url=request_link)
        content = json.loads(r.content)
        if content['code'] != '0':
            print(name + ' pass')
            continue
        data = content['data']
        data_all += data
        print(datetime.datetime.fromtimestamp(int(data[-1][0]) // 1000))

        for _ in tqdm(range(max_time)):
            after_time = data[-1][0]
            request_link = f"https://www.okex.com/api/v5/market/history-candles?instId=" + name + "&after=" + after_time
            r = requests.get(url=request_link)
            content = json.loads(r.content)
            data = content['data']
            data_all += data
            # print(datetime.datetime.fromtimestamp(int(data[0][0]) // 1000))
            # print(datetime.datetime.fromtimestamp(int(data[-1][0]) // 1000))

        df = pd.DataFrame(data_all)
        df.columns = ['time', 'open', 'high', 'low', 'close', 'vol', 'volCcy']
        df.sort_values('time', ascending=True).to_csv(os.path.join(save_base, name + '.csv'), index=None)


def get_index_history(names, max_time, begin_time=None):
    if begin_time is None:
        begin_time = 1621210920000
    for name in names:
        data_all = []
        if begin_time is not None:
            request_link = f"https://www.okex.com/api/v5/market/index-candles?instId=" + name + '&after=' + str(begin_time)
        print("Request link: " + request_link)
        r = requests.get(url=request_link)
        content = json.loads(r.content)
        if content['code'] != '0' and content['data'] != []:
            print(name + ' pass')
            continue
        data = content['data']
        data_all += data
        print(datetime.datetime.fromtimestamp(int(data[-1][0]) // 1000))

        for _ in tqdm(range(max_time)):
            after_time = data[-1][0]
            request_link = f"https://www.okex.com/api/v5/market/index-candles?instId=" + name + "&after=" + after_time
            r = requests.get(url=request_link)
            content = json.loads(r.content)
            data = content['data']
            if data == []:
                break
            data_all += data
            # print(datetime.datetime.fromtimestamp(int(data[0][0]) // 1000))
            # print(datetime.datetime.fromtimestamp(int(data[-1][0]) // 1000))

        df = pd.DataFrame(data_all)
        df.columns = ['time', 'open', 'high', 'low', 'close']
        df.sort_values('time', ascending=True).to_csv(os.path.join(save_base, name + '.csv'), index=None)


if __name__ == '__main__':
    names = ['BTC-USDT', 'ETH-USDT', 'LTC-USDT', 'ETC-USDT', 'XRP-USDT', 'EOS-USDT', 'BCH-USDT', 'BSV-USDT', 'TRX-USDT',
             'OKB-USDT']
	# swap是合约，没swap的是现货
    names2 = [tmp + '-SWAP' for tmp in names]
    names = names + names2
    save_base = 'data'
    if not os.path.exists(save_base):
        os.mkdir(save_base)
    max_time = 1300
    get_history(names, max_time)
    names = ['BTC-USD', 'ETH-USD', 'LTC-USD', 'ETC-USD', 'XRP-USD', 'EOS-USD', 'BCH-USD', 'BSV-USD', 'TRX-USD', 'OKB-USD']
    get_index_history(names, max_time)

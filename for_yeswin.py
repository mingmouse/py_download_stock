import pandas as pd
import os
import numpy as np
def calculate_moving_average(df, window):
    return df['closing_price'].rolling(window=window).mean()

def convert_to_gregorian_date(minguo_date):
    # 將日期字符串分割為年、月、日
    year, month, day = map(int, minguo_date.split('/'))
    # 將民國年份轉換為西元年份
    year += 1911
    return pd.Timestamp(year, month, day)
    
def convert_to_float(value):
    try:
        return float(value.replace(',', ''))
    except AttributeError:  # 如果值不是字符串（例如，如果它已經是數字），則無法調用 replace
        return value    
# 定義一個函數來讀取並整合每月的數據
def process_monthly_data(year, month, stock_id):
    filename = f'day/{stock_id}/{year}-{month:02d}.csv'
    if not os.path.exists(filename):
        return None
    
    monthly_data = pd.read_csv(filename, thousands=',')
    monthly_data.replace('--', np.nan, inplace=True)

    monthly_data['date'] = monthly_data['date'].apply(convert_to_gregorian_date)

# 應用 convert_to_float 函數於每一列
    columns_to_convert = ['volume', 'turnover', 'transaction', 'opening_price', 'closing_price', 'highest_price', 'lowest_price']
    for column in columns_to_convert:
        monthly_data[column] = monthly_data[column].apply(convert_to_float)
   
    monthly_data.replace('--', np.nan, inplace=True)
    monthly_data.dropna(inplace=True) 
    #print(monthly_data)
    # 計算月統計數據
    try:
        opening_price = monthly_data['opening_price'].iloc[0]
        closing_price = monthly_data['closing_price'].iloc[-1]
        highest_price = monthly_data['highest_price'].max()
        lowest_price = monthly_data['lowest_price'].min()
        total_volume = monthly_data['volume'].sum()

        return pd.Series([opening_price, closing_price, highest_price, lowest_price, total_volume],
                        index=['opening_price', 'closing_price', 'highest_price', 'lowest_price', 'total_volume'])
    except:
        return None
def read_stock_ids(filename):
            with open(filename, 'r') as file:
                stock_ids = file.read().splitlines()
            return stock_ids   
def fetch_stock_data(stock_id):
    start_year = 2012
    end_year = 2024

    all_monthly_data = []

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            monthly_stats = process_monthly_data(year, month, stock_id)
            if monthly_stats is not None:
                monthly_stats['year'] = year
                monthly_stats['month'] = month
                all_monthly_data.append(monthly_stats)

    # 轉換為 DataFrame
    monthly_df = pd.DataFrame(all_monthly_data)
    if monthly_df.empty:
         #print("No monthly data")
         return  {
                "符合次數": 0,
                "不符合次數": 0
            }
    else:     
        # 計算移動平均線
        monthly_df['ma5'] = calculate_moving_average(monthly_df, 5)
        monthly_df['ma10'] = calculate_moving_average(monthly_df, 10)
        monthly_df['ma20'] = calculate_moving_average(monthly_df, 20)
        # 確保 lowest_price 是浮點數
        monthly_df['lowest_price'] = pd.to_numeric(monthly_df['lowest_price'], errors='coerce')

        # 確保 MA 列也是浮點數
        monthly_df['ma5'] = pd.to_numeric(monthly_df['ma5'], errors='coerce')
        monthly_df['ma10'] = pd.to_numeric(monthly_df['ma10'], errors='coerce')
        monthly_df['ma20'] = pd.to_numeric(monthly_df['ma20'], errors='coerce')

        # 現在進行比較
        # 判斷條件
        monthly_df['condition_1'] = monthly_df['lowest_price'] > monthly_df[['ma5', 'ma10', 'ma20']].max(axis=1)

        monthly_df['prev_ma5'] = monthly_df['ma5'].shift(1)
        monthly_df['prev_ma10'] = monthly_df['ma10'].shift(1)
        monthly_df['prev_ma20'] = monthly_df['ma20'].shift(1)

        monthly_df['condition_2'] = (monthly_df['ma5'] > monthly_df['prev_ma5']) & (monthly_df['ma10'] > monthly_df['prev_ma10']) & (monthly_df['ma20'] > monthly_df['prev_ma20'])

        monthly_df['prev_volume'] = monthly_df['total_volume'].shift(1) 
        monthly_df['condition_3'] = (monthly_df['total_volume'] > 1.5 * monthly_df['prev_volume']) & (monthly_df['total_volume'] < 2 * monthly_df['prev_volume'])
        monthly_df['final_condition'] = monthly_df['condition_1'] & monthly_df['condition_2'] & monthly_df['condition_3']

        selected_months = monthly_df[monthly_df['final_condition']]
        
        return calculate_price_increase(selected_months[['year', 'month', 'opening_price', 'closing_price', 'highest_price', 'lowest_price', 'total_volume', 'ma5', 'ma10', 'ma20']],monthly_df)
        
def calculate_price_increase(selected_months, all_monthly_data):
    # 初始化計數器
    stats = {
        "符合次數": 0,
        "不符合次數": 0
    }

    # 遍歷每個符合條件的月份
    for index, row in selected_months.iterrows():
        # 獲取起始月份和年份
        start_year = row['year']
        start_month = row['month']

        # 計算一年後的月份和年份
        end_year = start_year + 1
        end_month = start_month

        # 從所有數據中篩選出這一年內的數據
        year_data = all_monthly_data[
            (all_monthly_data['year'] >= start_year) & 
            (all_monthly_data['year'] <= end_year) &
            (all_monthly_data['month'] >= start_month) & 
            (all_monthly_data['month'] <= end_month)
        ]
        
        year_data['highest_price'] = pd.to_numeric(year_data['highest_price'], errors='coerce')

        # 計算這一年內的最高價
        max_price = year_data['highest_price'].max()

        # 計算起始月份的收盤價
        
        start_price =  pd.to_numeric(row['closing_price'], errors='coerce')

        # 計算漲幅
        if start_price > 0:  # 避免除以0
            increase = (max_price - start_price) / start_price

            # 判斷是否滿足漲幅條件
            if increase >= 0.3:
                stats["符合次數"] += 1
            else:
                return {
                    "符合次數": 0,
                    "不符合次數": 0
                }

    return stats
# 主程式
import threading
stock_ids = read_stock_ids('stock_num.txt')
# 為每個股票代碼創建一個執行緒
total_stats = {
        
        "符合次數": 0,
        "不符合次數": 0
}
reslut = []
threads = []
for stock_id in stock_ids:
    
    select = fetch_stock_data(stock_id)
    if select['符合次數']>0:
        struct = {
            'stock_id':stock_id,
            'total_stats':select['符合次數'] > 1 
        }
        reslut.append(struct)
        #print(reslut)
       

#     thread = threading.Thread(target=fetch_stock_data, args=(stock_id,))
#     threads.append(thread)
#     thread.start()

# # 等待所有執行緒完成
# for thread in threads:
#     thread.join()

print()    
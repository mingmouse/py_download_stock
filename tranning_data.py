import pandas as pd
import numpy as np  # 確保導入 numpy

def convert_to_gregorian_date(minguo_date):
    # 將日期字符串分割為年、月、日
    year, month, day = map(int, minguo_date.split('/'))
    # 將民國年份轉換為西元年份
    year += 1911
    return pd.Timestamp(year, month, day)

def read_and_process_file(filename):
    df = pd.read_csv(filename, thousands=',')
    # 轉換日期格式
    df['date'] = df['date'].apply(convert_to_gregorian_date)
    df['volume'] = df['volume'].astype(float)
    df['volume'] = df['volume'].astype(float)
    df['turnover'] = df['turnover'].astype(float)
    df['transaction'] = df['transaction'].astype(float)
    return df

# 初始化一個空的 DataFrame
all_data = pd.DataFrame()

for year in range(2012, 2024):  # 從 2012 到 2017 年
    for month in range(1, 13):  # 每年的 12 個月
        filename = f'day/1104/{year}-{str(month).zfill(2)}.csv'  # 檔案名稱格式
        monthly_data = read_and_process_file(filename)
        all_data = pd.concat([all_data, monthly_data], ignore_index=True)

# 確保數據已經按日期排序
all_data.sort_values(by='date', inplace=True)
all_data.replace('--', np.nan, inplace=True)
all_data.dropna(inplace=True)  # 刪除含有 NaN 的行

print(all_data)
# 現在 all_data 包含了 2012 至 2017 年的完整數據
import pandas as pd
from sklearn import preprocessing
from keras.models import Sequential
from keras.layers import LSTM, Dense, Dropout
import numpy as np
import matplotlib.pyplot as plt
def create_dataset(df, feature_col_index, label_col_index, time_step=60):
    X, y = [], []
    for i in range(len(df) - time_step - 1):
        a = df.iloc[i:(i + time_step), feature_col_index].values
        X.append(a)
        y.append(df.iloc[i + time_step, label_col_index])
    return np.array(X), np.array(y)

# 重新生成数据集

# 假设 df 是包含股价的 NumPy 数组

# 步驟 1: 資料正規化
features = ['opening_price', 'highest_price', 'lowest_price', 'closing_price', 'volume']
all_data[features] = preprocessing.MinMaxScaler().fit_transform(all_data[features])

# 步驟 2: 訓練集和測試集的劃分
time_frame = 0
#result = []
#for index in range(len(all_data) - time_frame):
#    result.append(all_data[features].iloc[index: index + time_frame].values)
#result = np.array(result)

train_size = int(0.4 * len(all_data[features]))
#X_train = result[:train_size, :-1]
#y_train = result[:train_size, -1][:, -1]
#X_test = result[train_size:, :-1]
#y_test = result[train_size:, -1][:, -1]
time_step = 60
# 找到 'closing_price' 列的索引
feature_col_index = [all_data.columns.get_loc(col) for col in features]
label_col_index = all_data.columns.get_loc('highest_price')

# 重新生成数据集
X, y = create_dataset(all_data, feature_col_index, label_col_index, time_step)

X_train, y_train = X[:train_size], y[:train_size]
X_test, y_test = X[train_size:], y[train_size:]
# 步驟 3: 建立 LSTM 模型
def build_lstm_model(input_shape):
    model = Sequential()
    model.add(LSTM(units=50, return_sequences=True, input_shape=input_shape))
    model.add(Dropout(0.2))
    model.add(LSTM(units=50, return_sequences=False))
    model.add(Dropout(0.2))
    model.add(Dense(units=1))  # 因为我们预测的是一个值（如收盘价）

    model.compile(optimizer='adam', loss='mean_squared_error')
    return model

# 获取输入特征的维度
input_shape = (X_train.shape[1], X_train.shape[2])  # (time_step, number_of_features)

# 构建模型
model = build_lstm_model(input_shape)
# model = Sequential()
# model.add(LSTM(50, return_sequences=True, input_shape=(X_train.shape[1], X_train.shape[2])))
# model.add(Dropout(0.2))
# model.add(LSTM(50, return_sequences=False))
# model.add(Dropout(0.2))
# model.add(Dense(1))
# model.compile(optimizer='adam', loss='mean_squared_error')

# 訓練模型
#model.fit(X_train, y_train, epochs=50, batch_size=100, validation_data=(X_test, y_test), verbose=1)
model.fit(X_train, y_train, epochs=100, batch_size=32, validation_data=(X_test, y_test))
#
# 步驟 4: 進行預測並評估結果
predicted = model.predict(X_test)

# 步驟 5: 視覺化預測結果
plt.figure(figsize=(50, 30))
plt.plot(y_test, label='Actual Price')
plt.plot(predicted, label='Predicted Price')
plt.title('Stock Price Prediction')
plt.xlabel('Time')
plt.ylabel('Normalized Stock Price')
plt.legend()
plt.show()

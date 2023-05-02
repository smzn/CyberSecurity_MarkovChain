import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import time
import sys

#file = 'netflow_day-03'
file = sys.argv[1]
df = pd.read_csv(file + '_sum.csv', index_col=0)
df_device = pd.read_csv(file + '_device_intersection.csv')
print(len(df_device))
print('Import OK')

transition_count = np.zeros((len(df_device), len(df_device)))
#transition_duration = np.zeros((len(df_device), len(df_device)))
#transition_from_packets = np.zeros((len(df_device), len(df_device)))
#transition_to_packets = np.zeros((len(df_device), len(df_device)))
#transition_from_bytes = np.zeros((len(df_device), len(df_device)))
#transition_to_bytes = np.zeros((len(df_device), len(df_device)))
print('array OK')

#dfの項目(from, to, count, duration, from_packets, to_packets, from_bytes, to_bytes) これから一行ずつ取り出して、from, toで指定した場所に格納
device_list = df_device['Device'].tolist()
print('device_list OK')
index = 0
for i in df.itertuples():
    index += 1
    if i[1] in device_list:
        idx1 = device_list.index(i[1])
    else : 
        continue
    if i[2] in device_list:
        idx2 = device_list.index(i[2])
    else:
        continue
    transition_count[idx1, idx2] = i.count
    #transition_duration[idx1, idx2] = i.duration
    #transition_from_packets[idx1, idx2] = i.from_packets
    #transition_to_packets[idx1, idx2] = i.to_packets
    #transition_from_bytes[idx1, idx2] = i.from_bytes
    #transition_to_bytes[idx1, idx2] = i.to_bytes
    if index % 10000 == 0:
        print('{0}%処理済み'.format(index / len(df) * 100))            

df_count = pd.DataFrame(transition_count)
#df_duration = pd.DataFrame(transition_duration)
#df_from_packets = pd.DataFrame(transition_from_packets)
#df_to_packets = pd.DataFrame(transition_to_packets)
#df_from_bytes = pd.DataFrame(transition_from_bytes)
#df_to_bytes = pd.DataFrame(transition_to_bytes)
print('DataFrame OK')

#df_count.to_csv('transition_count.csv')
#np.savetxt('transition_count.csv', transition_count, fmt='%d', delimiter=',')
#np.savetxt('transition_duration.csv', transition_duration, fmt='%d', delimiter=',')
#np.savetxt('transition_from_packets.csv', transition_from_packets, fmt='%.4f', delimiter=',')
#np.savetxt('transition_to_packets.csv', transition_to_packets, fmt='%.4f', delimiter=',')
#np.savetxt('transition_from_bytes.csv', transition_from_bytes, fmt='%.4f', delimiter=',')
#np.savetxt('transition_to_bytes.csv', transition_to_bytes, fmt='%.4f', delimiter=',')
#print('transition_count Save OK')


#行和が0になるホストを削除(行と列を削除する) countを基準とする
row_sum = np.sum(df_count.values, axis=1).tolist()
row_zero_index = [i for i, val in enumerate(row_sum) if val == 0]
print('row_zero_index OK')
print(row_zero_index)
#device_list = device_list.copy()
de_index = 0
while len(row_zero_index) > 0: #行和が0になる要素がある限り繰り返す
    print('{0}回目'.format(de_index))
    df_count = df_count.drop(index=df_count.index[row_zero_index],columns=df_count.columns[row_zero_index]) #行と列から削除
    #df_duration = df_duration.drop(index=df_duration.index[row_zero_index],columns=df_duration.columns[row_zero_index])
    #df_from_packets = df_from_packets.drop(index=df_from_packets.index[row_zero_index],columns=df_from_packets.columns[row_zero_index])
    #df_to_packets = df_to_packets.drop(index=df_to_packets.index[row_zero_index],columns=df_to_packets.columns[row_zero_index])
    #df_from_bytes = df_from_bytes.drop(index=df_from_bytes.index[row_zero_index],columns=df_from_bytes.columns[row_zero_index])
    #df_to_bytes = df_to_bytes.drop(index=df_to_bytes.index[row_zero_index],columns=df_to_bytes.columns[row_zero_index])
    #削除したホストを集合に適用
    for i in sorted(row_zero_index, reverse=True):
        device_list.pop(i)
    row_sum = np.sum(df_count.values, axis=1).tolist() #行和を計算
    row_zero_index = [i for i, val in enumerate(row_sum) if val == 0] #行和が0となるリスト
    de_index += 1
    
print('Delete OK')

#推移確率行列にホスト名を追加
df_count.index=device_list
df_count.columns=device_list
#df_duration.index=device_list
#df_duration.columns=device_list
#df_from_packets.index=device_list
#df_from_packets.columns=device_list
#df_to_packets.index=device_list
#df_to_packets.columns=device_list
#df_from_bytes.index=device_list
#df_from_bytes.columns=device_list
#df_to_bytes.index=device_list
#df_to_bytes.columns=device_list

#csvに出力
#df_count.to_csv('transition_count_non0.csv')
#df_duration.to_csv('transition_duration_non0.csv')
#df_from_packets.to_csv('transition_from_packets_non0.csv')
#df_to_packets.to_csv('transition_to_packets_non0.csv')
#df_from_bytes.to_csv('transition_from_bytes_non0.csv')
#df_to_bytes.to_csv('transition_to_bytes_non0.csv')
df_device_list_non0 = pd.DataFrame(device_list)
df_device_list_non0.to_csv('device_list_non0.csv')
print('device_list save OK')

#推移確率行列に変換(行和を1にする)
transition_count_non0_rate = df_count.values.astype(np.float32) #整数型のままだと行和で割ったときエラー
row_sum = np.sum(transition_count_non0_rate, axis=1)
for i in range(len(transition_count_non0_rate)):
    transition_count_non0_rate[i] /= row_sum[i]
df_transition_count_non0_rate = pd.DataFrame(transition_count_non0_rate, index=device_list, columns=device_list)
df_transition_count_non0_rate.to_csv('transition_count_non0_rate.csv')
print('transition_count_non0 save OK')
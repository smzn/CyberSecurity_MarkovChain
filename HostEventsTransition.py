import pandas as pd
import time
from mpi4py import MPI
import numpy as np
import sys

class HostEventsTransition:
    def __init__(self, data_file, rank, size):
        self.df = pd.read_csv(data_file)
        print(self.df.head())
        self.rank = rank
        self.size = size

        self.EventID_list = self.df['EventID'].unique().tolist() #推移確率の状態空間
        self.EventID_list.append(-1)
        self.UserName_list = self.df['UserName'].unique().tolist() #利用者名
        print('ユニーク利用者数 : ' +str(len(self.UserName_list)))
        self.transition_EventID = np.zeros((len(self.EventID_list), len(self.EventID_list)))

    def getTransition(self):
        l_div = [(len(self.UserName_list)+i) // self.size for i in range(self.size)]
        print(l_div)
        start = 0
        end = 0
        for i in range(self.rank):
            start += l_div[i]
        for i in range(self.rank + 1):
            end += l_div[i]
        div_unique = self.UserName_list[start:end] #自分の担当の利用者リスト
        print('rank = {0}, size = {1}, start = {2}, end = {3}'.format(self.rank, self.size, start, end))

        index = 0
        for i in div_unique:
            df_each = self.df[self.df['UserName'] == i]
            idx1 = self.EventID_list.index(-1)
            for j in df_each.itertuples():
                idx2 = self.EventID_list.index(j.EventID)
                self.transition_EventID[idx1, idx2] += 1
                idx1 = idx2
            idx2 = self.EventID_list.index(-1)#終了は-1に戻るとする
            self.transition_EventID[idx1, idx2] += 1
            print('UserName : {0}, EventID Length : {1}'.format(i,len(df_each['EventID'])))
            print('処理済み {0}%'.format(index / len(div_unique)))
            index += 1
        
        #集約
        if self.rank == 0:
            for i in range(1, self.size):
                transition = self.comm.recv(source=i, tag=11)
                self.transition_EventID += np.array(transition)
        else:
            self.comm.send(self.transition_EventID, dest=0, tag=11)

        #推移確率行列に変換(行和を1にする)
        transition_EventID_rate = self.transition_EventID.astype(np.float32) #整数型のままだと行和で割ったときエラー
        row_sum = np.sum(transition_EventID_rate, axis=1)
        for i in range(len(transition_EventID_rate)):
            transition_EventID_rate[i] /= row_sum[i]
        df_transition_EventID_rate = pd.DataFrame(transition_EventID_rate, index=self.EventID_list, columns=self.EventID_list)
        df_transition_EventID_rate.to_csv('transition_EventID_rate.csv')
        print('transition_EventID_rate save OK')

if __name__ == '__main__':
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    data_file = sys.argv[1]
    transition = HostEventsTransition(data_file, rank, size)
    transition.getTransition()
    #mpiexec -n 6 python3 HostEventsTransition.py wls_day-07_all.csv > HostEventsTransition.txt
import pandas as pd
import time
from mpi4py import MPI
import numpy as np
import sys
import psutil

class HostEventsTransition:
    def __init__(self, data_file, rank, size, comm):
        self.df = pd.read_csv(data_file)
        print(self.df.head())
        self.rank = rank
        self.size = size
        self.comm = comm

        self.EventID_list = self.df['EventID'].unique().tolist() #推移確率の状態空間
        self.EventID_list.append(-1)
        self.UserName_list = self.df['UserName'].unique().tolist() #利用者名
        self.process_text = './process/process.txt'
        if self.rank == 0:
            with open(self.process_text, 'a') as f:
                print('ファイル名 : {0}'.format(data_file), file=f)
                print('ユニーク利用者数 : ' +str(len(self.UserName_list)), file=f)
                mem = psutil.virtual_memory()
                cpu = psutil.cpu_percent(interval=1)
                print('データ読み込み完了, Memory = {0}GB, CPU = {1}'.format(mem.used/10**9, cpu), file=f)
        self.transition_EventID = np.zeros((len(self.EventID_list), len(self.EventID_list)))

    def getTransition(self):
        l_div = [(len(self.UserName_list)+i) // self.size for i in range(self.size)]
        if self.rank == 0:
            with open(self.process_text, 'a') as f:
                print('利用者分割 : {0}'.format(l_div),file=f)
        start = 0
        end = 0
        for i in range(self.rank):
            start += l_div[i]
        for i in range(self.rank + 1):
            end += l_div[i]
        div_unique = self.UserName_list[start:end] #自分の担当の利用者リスト
        with open(self.process_text, 'a') as f:
            print('rank = {0}, size = {1}, start = {2}, end = {3}'.format(self.rank, self.size, start, end), file=f)

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
            with open(self.process_text, 'a') as f:
                print('UserName : {0}, EventID Length : {1}, rank = {2}'.format(i,len(df_each), self.rank), file=f)
                print('処理済み {0}%, rank = {1}'.format(index / len(div_unique) * 100, self.rank), file=f)
            index += 1
        
        #集約
        if self.rank == 0:
            for i in range(1, self.size):
                transition = self.comm.recv(source=i, tag=11)
                self.transition_EventID += np.array(transition)
        else:
            self.comm.send(self.transition_EventID, dest=0, tag=11)
        if self.rank == 0:
            with open(self.process_text, 'a') as f:
                print('集約完了', file=f)

        #推移確率行列に変換(行和を1にする)
        if self.rank == 0:
            transition_EventID_rate = self.transition_EventID.astype(np.float32) #整数型のままだと行和で割ったときエラー
            row_sum = np.sum(transition_EventID_rate, axis=1)
            for i in range(len(transition_EventID_rate)):
                transition_EventID_rate[i] /= row_sum[i]
            df_transition_EventID_rate = pd.DataFrame(transition_EventID_rate, index=self.EventID_list, columns=self.EventID_list)
            df_transition_EventID_rate.to_csv('transition_EventID_rate.csv')
            with open(self.process_text, 'a') as f:
                print('transition_EventID_rate save OK', file=f)

if __name__ == '__main__':
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    start = time.time()
    data_file = sys.argv[1]
    transition = HostEventsTransition(data_file, rank, size, comm)
    transition.getTransition()
    elapsed_time = time.time() - start
    if rank == 0:
        print ("calclation_time:{0}".format(elapsed_time) + "[sec]")
    #mpiexec -n 6 python3 HostEventsTransition.py wls_day-07_all.csv > HostEventsTransition.txt
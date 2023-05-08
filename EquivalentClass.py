import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def getEquivalence4(th, roop, p):
  reachable = np.zeros((len(p), len(p))) #全て0のpと同じ大きさの行列を用意
  equivalence = [[] for i in range(len(p))] #同値類を最大数用意
  list_number = 0

  for ix in range(roop): #n乗まで実施
    pn = np.linalg.matrix_power(p.copy(), ix+1) #累乗 
    for i in range(len(pn)):
      for j in range(len(pn)):
        if(pn[i][j] > th): #推移確率が閾値より大きいかチェック
          reachable[i,j] = 1

#  print('到達行列 = \n{0}'.format(reachable))
#  communicate = np.zeros((len(p), len(p))) #全て0のpと同じ大きさの行列を用意
#  for i in range(len(reachable)):
#    for j in range(len(reachable)):
#      if(reachable[i][j] == 1 and reachable[j][i] == 1):
    
  for i in range(len(pn)):
    for j in range(i+1, len(pn)):
      if(reachable[i][j] == 1 and reachable[j][i] == 1):
#        print('reachable {0} <-> {1}'.format(i, j))
        find = 0 #iでfind -> 1, jでfind -> 2
        idx = len(pn)
        for k in range(len(pn)):
          if i in equivalence[k]: #iが同値類k番目に存在している
            find = 1 #iは同値類k番目に存在
            idx = k
#            print('{0} find in {1}'.format(i, equivalence[k]))
            break
          elif j in equivalence[k]: 
            find = 2
            idx = k
#            print('{0} find in {1}'.format(j, equivalence[k]))
            break
        if find == 1:
          if j not in equivalence[idx]: #jは同値類kには存在しない (他のリストにもないことを確認する!!!他のリストにあった場合は移動がいい？ -> communicateがずれないように最後に集合で演算する)
            equivalence[idx].append(j) #jを追加
#            print('{0}に{1}を追加'.format(equivalence[idx], j))
#            print('リスト全体 {0}'.format(equivalence))     
            #break   
        elif find == 2:
          if i not in equivalence[idx]: #(他のリストにもないことを確認する!!!)
            equivalence[idx].append(i)
#            print('{0}に{1}を追加'.format(equivalence[idx], i))
#            print('リスト全体 {0}'.format(equivalence))         
            #break
        elif(find == 0): #新規に追加
          equivalence[list_number].append(i)
#          print('{0}に{1}を追加'.format(equivalence[list_number], i))
#          print('リスト全体 {0}'.format(equivalence))
          if(i != j):
            equivalence[list_number].append(j)
#            print('{0}に{1}を追加'.format(equivalence[list_number], j))
#            print('リスト全体 {0}'.format(equivalence))
          list_number += 1

  #4. Communicateにならないノードを登録
  for i in range(len(pn)):
    find = 0
    for j in range(len(pn)):
      if i in equivalence[j]:
        find = 1
        break
    if find == 0:
      equivalence[list_number].append(i)
#      print('Non Communication node {0} add'.format(i))
#      print('リスト全体 {0}'.format(equivalence))
      list_number += 1

  #5. エルゴード性の確認(class数が1のとき)
  class_number = 0
  for i in range(len(pn)):
    if len(equivalence[i]) > 0:
#      print('クラスの長さ : {0}, {1}'.format(len(equivalence[i]), equivalence[i]))
      class_number += 1

  for i in range(class_number):
    for j in range(i+1, class_number):
      s1 = set(equivalence[i])
      s2 = set(equivalence[j])
      if s1 & s2 :
#        print('重複 {0} & {1}'.format(i, j))
#        print('重複ノード {}'.format(s1 & s2))
        equivalence[i] = equivalence[i] + equivalence[j]
        equivalence[j].clear()

  #print('修正クラス数 {0}'.format(modify_class_number))
  for i in range(class_number):
  #  print(equivalence[i])
    equivalence[i] =list(set(equivalence[i]))

  #再度クラス数チェック
  class_number = 0
  for i in range(len(pn)):
    if len(equivalence[i]) > 0:
      class_number += 1
  
  #再度リストを構成
  modify_equivalence = [[] for i in range(class_number)] #同値類をクラス数用意
  l_index = 0
  for i in range(len(pn)):
    if len(equivalence[i]) > 0:
      modify_equivalence[l_index] = equivalence[i]
      l_index += 1

  return modify_equivalence, class_number, reachable

def getDegreeMax(equivalence, reachable):
  degree_max_value = [] #クラスの中で最大次数のものをクラス0から順番に入れる
  degree_max_index = []
  reachable_sum = np.sum(reachable, axis=0) + np.sum(reachable, axis=1) #到達行列から行和と列和の和
  for i in range(len(equivalence)): #クラス数だけ回す
    deg_max = -1 #最大値の初期値
    deg_max_index = -1 #最大値を持つノード番号
    for j in equivalence[i]: #ノード番号を一つずつ取り出す
      if deg_max < reachable_sum[j]:
        deg_max = reachable_sum[j]
        deg_index = j
    degree_max_value.append(deg_max)
    degree_max_index.append(deg_index)
  return degree_max_index, degree_max_value

if __name__ == '__main__':
    file = 'transition_count_non0_rate.csv'
    df = pd.read_csv(file, index_col=0)
    p_plus = 0.001
    roop = 1
    delta = 0
    p = df.values
    #次数中心性で実施した場合
    class_number_list = [] #各回のクラス数を保存
    degree_max_value_list = [] #各回のクラス毎の最大次数を保存
    class_each_number_list = [] #それぞれのクラスの要素数

    equivalence, class_number, reachable = getEquivalence4(delta, roop, p)
    print('クラス数 : {0}'.format(class_number))
    class_number_list.append(class_number)

    for j in range(class_number):
        print(equivalence[j])
        class_each_number_list.append(len(equivalence[j]))
        
    degree_max_index, degree_max_value = getDegreeMax(equivalence, reachable)
    print('各クラスの最大次数を持つノード番号 : {0}'.format(degree_max_index))
    print('各クラスの最大次数 : {0}'.format(degree_max_value))
    #degree_max_value_list.append(degree_max_value)

    #print(class_number_list)
    #print(class_each_number_list)
    #print(degree_max_value_list)     

    #最大のクラスサイズを持つリストを選択
    print('最大クラス要素数 : {0}'.format(max(class_each_number_list)))
    print('最大クラスを持つindex : {0}'.format(class_each_number_list.index(max(class_each_number_list))))
    print('最大クラスの最大次数を持つノード番号 : {0}、デバイス名 : {1}'.format(degree_max_index[class_each_number_list.index(max(class_each_number_list))], df.index[degree_max_index[class_each_number_list.index(max(class_each_number_list))]]))
    print('最大クラス数に所属するDevice_list番号 : {0}'.format(equivalence[class_each_number_list.index(max(class_each_number_list))]))
    maxclass_list = equivalence[class_each_number_list.index(max(class_each_number_list))]
    df_maxclass = df.iloc[maxclass_list, maxclass_list]
    df_maxclass.to_csv('transition_count_non0_rate_maxclass.csv')

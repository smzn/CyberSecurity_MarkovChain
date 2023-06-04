import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import time
from scipy.optimize import linprog
from discreteMarkovChain import markovChain

df = pd.read_csv('transition_count_non0_rate_maxclass.csv', index_col=0)

transition_matrix = df.values
print(transition_matrix.shape)

#https://pypi.org/project/discreteMarkovChain/
mc = markovChain(transition_matrix)
mc.computePi('linear') #We can also use 'power', 'krylov' or 'eigen'
print(mc.pi)
df2 = pd.DataFrame(
    data={'Device': df.index.tolist(), 
          'pi': mc.pi}
)
df2.to_csv('StationaryDistribution.csv')

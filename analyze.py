# %%
import numpy as np
from numpy.lib.function_base import disp
import pandas as pd
from numba import jit

import matplotlib.pyplot as plt
from matplotlib.pyplot import figure
import matplotlib.cm as cm 
# %%
def readRecord(id):
    recordFileName = f'reports/records/record-{id}.csv'
    return pd.read_csv(recordFileName, sep = ',', keep_default_na=False)

def readDependency(maxDependentTxns=10):
    dependencyFileName = "reports/dependency.csv"
    deps = pd.read_csv(dependencyFileName, error_bad_lines=False, header=None, names=range(maxDependentTxns))
    deps = deps.rename(columns={0: 'Transaction ID'}).iloc[1:, :]
    # deps = deps[1:]
    return deps

def readRecords(serverNum):
    recordList = [readRecord(serverId) for serverId in range(serverNum)]
    return pd.concat(recordList).sort_values(by='Transaction ID', ascending=True).replace(0, np.NaN)

def avgRecords(records):
    return np.mean(records)
    
# %%
records = readRecords(4)
print(f'Records with shape: {records.shape}')
display(records)

avgRecs = avgRecords(records)
print(f'Avg of Records with shape: {avgRecs.shape}')
display(avgRecs)

# %%
deps = readDependency()[:100000]
print(f'Dependency with shape: {deps.shape}')
display(deps)

# %%
def convertDatatype(deps, executionTimes, endTimestamp):
    depsNp = deps.to_numpy()
    executionTimesDict = executionTimes.set_index('Transaction ID')['Execution Time'].to_dict()
    endTimestampDict = endTimestamp.set_index('Transaction ID')['Txn End TimeStamp'].to_dict()

    return depsNp, executionTimesDict, endTimestampDict

@jit(forceobj=True)
def computePredPro(deps, executionTimes, endTimestamp):
    txnPredEndingTime = []
    txnNum = deps.shape[0]
    for idx in range(txnNum):
        # new_row = innerLoop(idx, deps, executionTimes, endTimestamp)
        row = deps[idx]
        # display(row[1])
        # print(row[1].shape)
        txn = row[0]
        # print(txn)
        depTxns = row[1:]
        depTxns = depTxns[~np.isnan(depTxns)].tolist()
        # print(depTxns)

        # depTimes = endTimestamp[endTimestamp['Transaction ID'].isin(depTxns)]['Txn End TimeStamp'].fillna(0)
        depTimes = []
        for depTxn in depTxns:
            depTimes.append(endTimestamp[depTxn])
        # print("Dep Times: ", depTimes)

        if len(depTimes) == 0:
            maxDepTime = 0
        else:    
            maxDepTime = np.max(depTimes)
        # print("Max Dep Time: ", maxDepTime)

        # txnExeTime = executionTimes.loc[executionTimes['Transaction ID'] == txn, 'Execution Time'].to_numpy()[0]
        txnExeTime = executionTimes[txn]
        # print(txnExeTime)

        # actualTxnExeTime = endTimestamp.loc[endTimestamp['Transaction ID'] == txn, 'Txn End TimeStamp'].fillna(0).to_numpy()[0]
        actualTxnExeTime = endTimestamp[txn]

        # new_row = pd.DataFrame([[txn, maxDepTime + txnExeTime, actualTxnExeTime]], columns=['Transaction ID', 'Predicted Ending Time', 'Actual Ending Time'])
        new_row = [txn, maxDepTime + txnExeTime, actualTxnExeTime]
        txnPredEndingTime.append(new_row)

    return np.array(txnPredEndingTime)
# %%
# print(re0['Execution Time'])
# print(re0[['Execution Time', 'Get locks']])
# print(re0.loc[:, ['Execution Time', 'Get locks']])

# print(re0.iloc[1])
# print(re0.iloc[:, [1]])
# print(re0.iloc[:, [1, 2]])
# %%
executionTimes = records[['Transaction ID', 'Execution Time']]
display(executionTimes)
endTimestamps = records[['Transaction ID', 'Txn End TimeStamp']]
display(endTimestamps)
depsNp, executionTimesDict, endTimestampDict = convertDatatype(deps, executionTimes, endTimestamps)
# %%
predEndTimes = pd.DataFrame(computePredPro(depsNp, executionTimesDict, endTimestampDict)
                                , columns=['Transaction ID', 'Predicted Ending Time', 'Actual Ending Time'])
# %%
seqs = np.linspace(0, predEndTimes.shape[0] - 1, predEndTimes.shape[0])
predEndTimes['TxNum'] = seqs
# %%
# trim = predEndTimes
trim = predEndTimes.loc[predEndTimes['Predicted Ending Time'] > 10000000, :]
display(trim)

fig = figure(figsize=(16, 8), dpi=120)
# ax = fig.gca(projection='3d')

sc = plt.scatter(trim['Predicted Ending Time'], trim['Actual Ending Time'], c=trim['TxNum'])
colorBar = plt.colorbar(sc)
plt.xlabel("Predicted Ending Time in micro-sec")
plt.ylabel("Actual Ending Time in micro-sec")
plt.title("The scatter plot of Predicted Ending Time and Actual Ending Time")

plt.legend()
plt.tight_layout()
plt.grid()

fig.savefig("regression.png", facecolor='white', edgecolor='white')
plt.show()

# %%

# %%
import os 

import numpy as np
from numpy.core.records import record
from numpy.lib.function_base import disp
import pandas as pd
from numba import jit

from IPython.display import display

import matplotlib.pyplot as plt
from matplotlib.pyplot import figure
import matplotlib.cm as cm 
# %%
def readRecord(path, id):
    # recordFileName = f'records/record-{id}.csv'
    recordFileName = os.path.join(path, f'records/record-{id}.csv')

    # return pd.read_table(recordFileName, sep = ',', keep_default_na=False, na_values='')
    return pd.read_csv(recordFileName, sep = ',', keep_default_na=False, na_values=[], na_filter=False)

def readDependency(path, maxDependentTxns=10):
    dependencyFileName = os.path.join(path, "dependency.csv")
    deps = pd.read_csv(dependencyFileName, error_bad_lines=False, header=None, names=range(maxDependentTxns))
    deps = deps.rename(columns={0: 'Transaction ID'}).iloc[1:, :]
    isAscending = deps['Transaction ID'].is_monotonic_increasing
    if isAscending:
        boolStr = "True"
    else:
        boolStr = "False"
    print("Is the dependency file Monotonic Increasing: ", boolStr)
    # deps = deps[1:]
    return deps

def readRecords(path, serverNum):
    recordList = [readRecord(path, serverId) for serverId in range(serverNum)]
    return pd.concat(recordList).sort_values(by='Transaction ID', ascending=True).replace(0, np.NaN)

def avgRecords(records, path, isOutputFile=False):
    avg = np.mean(records)
    avgFilePath = os.path.join(path, 'avg.csv')
    if isOutputFile:
        avg.to_csv(avgFilePath)
    return avg
# %%
def convertDatatype(deps, records):
    depsNp = deps.to_numpy()

    executionTimes = records[['Transaction ID', 'Execution Time']]
    # executionTimes['Execution Time'] = executionTimes['Execution Time'] - records['Read from cache'].fillna(0) - records['Get locks'].fillna(0)
    executionTimes['Execution Time'] = executionTimes['Execution Time']
    print("Execution Time with shape: ", executionTimes.shape)
    display(executionTimes)

    startTimestamps = records[['Transaction ID', 'Txn Start TimeStamp']]
    print("Start Timestamps with shape: ", startTimestamps.shape)
    display(startTimestamps)

    endTimestamps = records[['Transaction ID', 'Txn End TimeStamp']]
    print("End Timestamps with shape: ", endTimestamps.shape)
    display(endTimestamps)

    executionTimesDict = executionTimes.set_index('Transaction ID')['Execution Time'].to_dict()
    startTimestampDict = startTimestamps.set_index('Transaction ID')['Txn Start TimeStamp'].to_dict()
    endTimestampDict = endTimestamps.set_index('Transaction ID')['Txn End TimeStamp'].to_dict()

    return depsNp, executionTimesDict, startTimestampDict, endTimestampDict

# def computePredSingal(txn, depTxns, txnPredEndingTime, txnPredEndingTimeDic, deps, executionTimes, startTimestamp, endTimestamp):
#     depTxns = depTxns[~np.isnan(depTxns)].tolist()
#     # print(depTxns)

#     depTimes = []
#     for depTxn in depTxns:
#         # depTimes.append(endTimestamp[depTxn])
#         endTime = txnPredEndingTimeDic.get(depTxn, None)
#         if endTime != None:
#             depTimes.append(endTime)
#     # print("Dep Times: ", depTimes)

#     if len(depTimes) == 0:
#         maxDepTime = txnStartTime
#     else:    
#         maxDepTime = np.max(depTimes)
#         # If the dependent Txns finished before Txn starts, then take "maxDepTime" as the start time of Txn.
#         if maxDepTime < txnStartTime:
#             maxDepTime = txnStartTime
#     # print("Max Dep Time: ", maxDepTime)

#     txnExeTime = executionTimes[txn]
#     # print(txnExeTime)

#     actualTxnExeTime = endTimestamp[txn]

#     # Convert to long integer
#     newTxnLong = np.int64(txn)
#     newEndTimeLong = np.int64(maxDepTime) + np.int64(txnExeTime)
#     newActualEndTimeLong = np.int64(actualTxnExeTime)
#     new_row = [newTxnLong, newEndTimeLong, newActualEndTimeLong]
    
#     # Update list and dict of predicted ending timestamp
#     txnPredEndingTime.append(new_row)
#     txnPredEndingTimeDic[newEndTimeLong] = newEndTimeLong

#     return newEndTimeLong, txnPredEndingTime, txnPredEndingTimeDic

@jit(forceobj=True)
def computePredPro(deps, executionTimes, startTimestamp, endTimestamp):
    txnPredEndingTime = []
    txnPredEndingTimeDic = {}
    txnNum = deps.shape[0]
    for idx in range(txnNum):
        row = deps[idx]
        # print("Row: ", row)

        txn = row[0]
        # print(txn)
        txnStartTime = startTimestamp[txn]

        depTxns = row[1:]
        depTxns = depTxns[~np.isnan(depTxns)].tolist()
        # print(depTxns)

        depTimes = []
        for depTxn in depTxns:
            depTimes.append(endTimestamp[depTxn])
            # endTime = txnPredEndingTimeDic.get(depTxn, None)
            # if endTime != None:
            #     depTimes.append(endTime)
            # else:
            #     depTimes.append(0)
        # print("Dep Times: ", depTimes)

        if len(depTimes) == 0:
            maxDepTime = txnStartTime
        else:    
            maxDepTime = np.max(depTimes)
            # If the dependent Txns finished before Txn starts, then take "maxDepTime" as the start time of Txn.
            if maxDepTime < txnStartTime:
                maxDepTime = txnStartTime
        # print("Max Dep Time: ", maxDepTime)

        txnExeTime = executionTimes[txn]
        # print(txnExeTime)

        actualTxnExeTime = endTimestamp[txn]

        # Convert to long integer
        newTxnLong = np.int64(txn)
        newEndTimeLong = np.int64(maxDepTime) + np.int64(txnExeTime)
        newActualEndTimeLong = np.int64(actualTxnExeTime)
        new_row = [newTxnLong, newEndTimeLong, newActualEndTimeLong]
        
        # Update list and dict of predicted ending timestamp
        txnPredEndingTime.append(new_row)
        txnPredEndingTimeDic[newEndTimeLong] = newEndTimeLong

    return np.array(txnPredEndingTime)

def plot(path, predEndTimes):
    seqs = np.linspace(0, predEndTimes.shape[0] - 1, predEndTimes.shape[0])
    predEndTimes['TxNum'] = seqs

    trim = predEndTimes
    # trim = predEndTimes.loc[predEndTimes['Predicted Ending Time'] > 10000000, :]
    print("Table for plotting regression with shape: ", trim.shape)
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

    figFileName = os.path.join(path, "regression.png")
    fig.savefig(figFileName, facecolor='white', edgecolor='white')
    plt.show()

def regression(path, recNum, isOutputAvgFile, analyzeTxnNum=None):
    records = readRecords(path, recNum)
    print(f'Records with shape: {records.shape}')
    display(records)

    avgRecs = avgRecords(records, path, isOutputFile=isOutputAvgFile)
    print(f'Avg of Records with shape: {avgRecs.shape}')
    display(avgRecs)

    depsFull = readDependency(path, maxDependentTxns=10)
    if analyzeTxnNum != None:
        deps = depsFull[:analyzeTxnNum]
    print(f'Dependency with shape: {deps.shape}')
    display(deps)

    depsNp, executionTimesDict, startTimestampDict, endTimestampDict = convertDatatype(deps, records)

    predEndTimesNp = computePredPro(depsNp, executionTimesDict, startTimestampDict, endTimestampDict)
    predEndTimes = pd.DataFrame(predEndTimesNp, columns=['Transaction ID', 'Predicted Ending Time', 'Actual Ending Time'])

    plot(path, predEndTimes)
# %%
# print(re0['Execution Time'])
# print(re0[['Execution Time', 'Get locks']])
# print(re0.loc[:, ['Execution Time', 'Get locks']])

# print(re0.iloc[1])
# print(re0.iloc[:, [1]])
# print(re0.iloc[:, [1, 2]])
# %%
if __name__ == "__main__":
    # regression("reports/RTE_1600", 4, isOutputAvgFile=True, analyzeTxnNum=1000000)
    path = "reports/default"
    recNum = 4
    isOutputAvgFile = True
    analyzeTxnNum = 100000
# %%
    records = readRecords(path, recNum)
    print(f'Records with shape: {records.shape}')
    display(records)

    avgRecs = avgRecords(records, path, isOutputFile=isOutputAvgFile)
    print(f'Avg of Records with shape: {avgRecs.shape}')
    display(avgRecs)
# %%
    depsFull = readDependency(path, maxDependentTxns=10)
    if analyzeTxnNum != None:
        deps = depsFull[:analyzeTxnNum]
    print(f'Dependency with shape: {deps.shape}')
    display(deps)

    depsNp, executionTimesDict, startTimestampDict, endTimestampDict = convertDatatype(deps, records)
# %%
    predEndTimesNp = computePredPro(depsNp, executionTimesDict, startTimestampDict, endTimestampDict)
    predEndTimes = pd.DataFrame(predEndTimesNp, columns=['Transaction ID', 'Predicted Ending Time', 'Actual Ending Time'])

    plot(path, predEndTimes)
# %%

# %%
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
# %%
def readRecord(id):
    recordFileName = f'reports/records/record-{id}.csv'
    return pd.read_csv(recordFileName)

def readDependency(maxDependentTxns=10):
    dependencyFileName = "reports/dependency1.csv"
    deps = pd.read_csv(dependencyFileName, error_bad_lines=False, header=None, names=range(maxDependentTxns))
    deps.rename(columns={'0': 'Transaction ID'}, inplace=True)
    return deps

def readRecords(serverNum):
    recordList = [readRecord(serverId) for serverId in range(serverNum)]
    return pd.concat(recordList).sort_values(by='Transaction ID', ascending=True).replace(0, np.NaN)

def avgRecords(records):
    return np.mean(records)
    
# %%
# re0 = readRecord(0)[-1000:]
records = readRecords(4)
print(f'Records with shape: {records.shape}')
display(records)

avgRecs = avgRecords(records)
print(f'Avg of Records with shape: {avgRecs.shape}')
display(avgRecs)

# %%
deps = readDependency()
print(f'Dependency with shape: {deps.shape}')
display(deps)

# %%
def computePred(deps, executionTimes, endTimestampe):
    txnPredEndingTime = pd.DataFrame(columns=['Transaction ID', 'Predicted Ending Time'])
    for row in deps.iterrows():
        txn = row['Transaction ID']
        depTxns = row.iloc[:, 1:]
        maxDepTime = np.max(endTimestampe[endTimestampe['Transaction ID'].isin(depTxns)])
        txnExeTime = executionTimes.loc[txn, :]

        new_row = pd.DataFrame([txn, maxDepTime + txnExeTime], columns=['Transaction ID', 'Predicted Ending Time'])
        txnPredEndingTime.append(new_row)
    return txnPredEndingTime

# %%
print(re0['Execution Time'])
print(re0[['Execution Time', 'Get locks']])
print(re0.loc[:, ['Execution Time', 'Get locks']])

print(re0.iloc[1])
print(re0.iloc[:, [1]])
print(re0.iloc[:, [1, 2]])

# %%
# print(re0[re0.iloc[:, [1]] != 0])
print(np.mean(re0))
# %%

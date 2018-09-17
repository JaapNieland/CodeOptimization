import pandas as pd
import time
import os

# time keeping ready?
startTime = time.process_time_ns()

# get list of all files
files = os.listdir("C:/Users/janieland/Documents/Development/Python/CodeOptimization/database/output/")

# initialize
transactionsSend = pd.DataFrame()
transactionsReceived = pd.DataFrame()

# loop through all files
for filename in files:
    # read the file
    filepath = "C:/Users/janieland/Documents/Development/Python/CodeOptimization/database/output/" + filename
    transactionsLeg = pd.read_csv(filepath, sep=';')

    # append to the receiver frame if receiver file
    if(filename.startswith("receiver")):
        transactionsReceived = pd.concat([transactionsReceived, transactionsLeg], ignore_index=True)

    # append to sender frame if sender file
    elif(filename.startswith("sender")):
        transactionsSend = pd.concat([transactionsSend, transactionsLeg], ignore_index=True)


# join the sender and receiver data frames
transactions = transactionsSend.join(transactionsReceived, on="transactionID", lsuffix="send", rsuffix="rec")

# find the transactions to the moon
toTheMoon = (transactions.receiverCountryISO == "TMN").values

# select only the moon transactions
transactionsToTheMoon = transactions.loc[toTheMoon]

# stopping time
finishTime = time.process_time_ns()
print(f'Executed sequence in {(finishTime-startTime)/1000000} milliseconds')
print(transactionsToTheMoon)

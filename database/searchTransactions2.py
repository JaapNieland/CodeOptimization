import pandas as pd
import time
import os

# time keeping ready?
startTime = time.process_time_ns()

# get list of all files
files = os.listdir("C:/Users/janieland/Documents/Development/Python/CodeOptimization/database/output")

# initialize
transactionsToTheMoon = pd.DataFrame()

# loop through all files
for filename in files:

    # check if the file contains the receiver part
    if(filename.startswith("receiver")):
        # read file in
        filepathReceiver = "C:/Users/janieland/Documents/Development/Python/CodeOptimization/database/output/" + filename        # build full path
        receiverLeg = pd.read_csv(filepathReceiver, sep=';')      # actual read in

        # check if there are any transactions from IRAN
        transactionInScope = receiverLeg.receiverCountryISO == 'TMN'
        if(sum(transactionInScope) > 0):
            # select transactions to the moon
            transactionsRec = receiverLeg[transactionInScope]

            # read receiver file
            filepathSend = filepathReceiver.replace("receiver", "sender")
            senderLeg = pd.read_csv(filepathSend, sep=";")

            transactionsToTheMoon = pd.concat([
                transactionsToTheMoon,
                pd.merge(transactionsRec, senderLeg, on="transactionID", how="left")
            ], ignore_index=True)


# stop time keeping
finishTime = time.process_time_ns()
print(f'Executed sequence in {(finishTime-startTime)/1000000} milliseconds')

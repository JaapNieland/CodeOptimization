import pandas as pd                 # pandas for data frames and IO
import numpy as np                  # numpy just to be awesome

# config
batchSize = 1000                     # number of transaction in one file
nBatches = 1000                      # number of files/2 (per transaction sender and receiver file)

# read input data
maleNames = pd.read_csv("database/resources/male-names.txt", skiprows=5, header=None)
femaleNames = pd.read_csv("database/resources/female-names.txt", skiprows=5, header=None)
lastNames = pd.read_csv("database/resources/last-names.txt", skiprows=0, header=None)
countries = pd.read_csv("database/resources/countries.csv", skiprows=0)

# pre processing
firstNames = pd.concat([maleNames, femaleNames], ignore_index=True)     # put male and females together
nFirstNames = firstNames.shape[0]                                       # count the number of first names
nLastNames = lastNames.shape[0]                                         # count the number of last names

# create all the batches
for itt in range(0, nBatches):
    # create the sender leg
    senderFrame = pd.DataFrame(
        {
            'transactionID': np.arange(0, batchSize) + int(itt)*batchSize,
            'senderFirstName': firstNames.sample(batchSize, replace=True).values.flatten(),
            'senderLastName': lastNames.sample(batchSize, replace=True).values.flatten(),
            'senderAccount': np.random.randint(low=1000000, high=9999999, size=batchSize),
            'senderCountry': countries['name'].sample(batchSize, replace=True).values.flatten(),
            'senderCountryISO': countries['alpha-3'].sample(batchSize, replace=True).values.flatten(),
            'amount': np.random.randint(low=100, high=100000, size=batchSize)
        }
    )

    # create the receiver leg
    receiverFrame = pd.DataFrame(
        {
            'transactionID': np.arange(0, batchSize) + int(itt)*batchSize,
            'receiverFirstName': firstNames.sample(batchSize, replace=True).values.flatten(),
            'receiverLastName': lastNames.sample(batchSize, replace=True).values.flatten(),
            'receiverAccount': np.random.randint(low=1000000, high=9999999, size=batchSize),
            'receiverCountry': countries['name'].sample(batchSize, replace=True).values.flatten(),
            'receiverCountryISO': countries['alpha-3'].sample(batchSize, replace=True).values.flatten(),
        }
    )

    # create interesting transaction potentially
    if(np.random.randint(10) < 2):
        changeLine = np.random.randint(low=0, high=batchSize)
        receiverFrame['receiverCountry'][changeLine] = "The Moon"
        receiverFrame['receiverCountryISO'][changeLine] = "TMN"





    # write results
    senderFilePath = "database/output/sender"+str(itt)+'.csv'
    receiverFilePath = "database/output/receiver"+str(itt)+'.csv'
    senderFrame.to_csv(senderFilePath, sep=";", index=False)
    receiverFrame.to_csv(receiverFilePath, sep=";", index=False)


import pandas as pd
import numpy as np

# config
batchSize = 200
nBatches = 100

# read input data
maleNames = pd.read_csv("database/resources/male-names.txt", skiprows=5, header=None)
femaleNames = pd.read_csv("database/resources/female-names.txt", skiprows=5, header=None)
lastNames = pd.read_csv("database/resources/last-names.txt", skiprows=0, header=None)
countries = pd.read_csv("database/resources/countries.csv", skiprows=0)

# pre processing
firstNames = pd.concat([maleNames, femaleNames], ignore_index=True)
nFirstNames = firstNames.shape[0]
nLastNames = lastNames.shape[0]
countries = countries[['name', 'alpha-3', 'region']]

identifier = np.arange(0, batchSize)
amount = pd.DataFrame({'amount': np.random.randint(low=100, high=100000, size= batchSize)})
fNamesSend = firstNames.sample(batchSize)
lNamesSend = lastNames.sample(batchSize)
accountSend = np.random.randint(low=1000000, high=9999999, size=(1, batchSize))
countrySend = countries.sample(batchSize)
fNamesRec = firstNames.sample(batchSize)
lNamesRec = lastNames.sample(batchSize)
countryRec = countries.sample(batchSize)

# create all the batches
for itt in range(0, nBatches):
    # create the sender leg
    senderFrame = pd.DataFrame(
        {
            'transactionID': np.arange(0, batchSize) + int(itt)*batchSize,
            'senderFirstName': firstNames.sample(batchSize).values.flatten(),
            'senderLastName': lastNames.sample(batchSize).values.flatten(),
            'senderAccount': np.random.randint(low=1000000, high=9999999, size=batchSize),
            'senderCountry': countries['name'].sample(batchSize).values.flatten(),
            'senderCountryISO': countries['alpha-3'].sample(batchSize).values.flatten(),
            'amount': np.random.randint(low=100, high=100000, size=batchSize)
        }
    )

    # create the receiver leg
    receiverFrame = pd.DataFrame(
        {
            'transactionID': np.arange(0, batchSize) + int(itt)*batchSize,
            'receiverFirstName': firstNames.sample(batchSize).values.flatten(),
            'receiverLastName': lastNames.sample(batchSize).values.flatten(),
            'receiverAccount': np.random.randint(low=1000000, high=9999999, size=batchSize),
            'receiverCountry': countries['name'].sample(batchSize).values.flatten(),
            'receiverCountryISO': countries['alpha-3'].sample(batchSize).values.flatten(),
            'amount': np.random.randint(low=100, high=100000, size=batchSize)
        }
    )

    # write results
    senderFilePath = "database/output/sender"+str(itt)+'.csv'
    receiverFilePath = "database/output/receiver"+str(itt)+'.csv'
    senderFrame.to_csv(senderFilePath, sep=";", index=False)
    receiverFrame.to_csv(receiverFilePath, sep=";", index=False)


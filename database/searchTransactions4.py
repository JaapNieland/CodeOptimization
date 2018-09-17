import pandas as pd
import multiprocessing as mp
import time
import glob

nProcesses = 1

class TransactionFilterer:

    def __init__(self, receiver_file):
        self.full_transactions = pd.DataFrame()
        self.sender_leg = pd.DataFrame()

        self.filepath_receiver = receiver_file
        self.filepath_sender = self.filepath_receiver.replace("receiver", "sender")

        self.receiver_leg = pd.read_csv(self.filepath_receiver, sep=";")

        # check if there are any transactions from the moon
        self.transaction_in_scope = self.receiver_leg.receiverCountryISO == 'TMN'

        # count transactions of interest
        self.nOfInterest = sum(self.transaction_in_scope)

    def processSender(self):
        self.sender_leg = pd.read_csv(self.filepath_sender, sep=";")
        self.full_transactions = pd.merge(self.receiver_leg[self.transaction_in_scope],
                                          self.sender_leg,
                                          on="transactionID",
                                          how="left")


def filterReceiverMoon(filepaths, q):
    """
    Filters the receiver files and checks if anything went to the moon. If that is the case,
    these transactions are put on the queue for another thread to process.
    :param filepaths:    the path to a file with a receiver leg
    :param q:            the queue that is going to be used for process communication
    :return:             None
    """

    # loop through all received files
    for filepath_receiver in filepaths:
        tf = TransactionFilterer(filepath_receiver)

        # check if there are any transactions left
        if tf.nOfInterest > 0:
            # let's quickly let the next worker know what file to read
            q.put(tf)

    # put a marker on the queue to let the next step know: nothing else is coming
    q.put(-1)


def mergeSenderMoon(q):
    """
    Reads the sender leg of a transaction from the queue and merges it to the receiver part

    :param q:   The data input queue
    :return:
    """

    # initialize empty data frame
    transactionsToTheMoon = pd.DataFrame()

    processes_finished = 0

    # keep looping until the loop is broken out of
    while True:
        # get the file path from the queue
        tf = q.get()

        # check if this is not the "we're done" path
        if tf == -1:
            processes_finished += 1
            if processes_finished == 4:
                print("everything done")
                break
            else:
                print(f"{processes_finished} processes are finished.")
                continue

        # read the sender leg
        tf.processSender()

        # merge the sending and receiving part
        transactionsToTheMoon = pd.concat([
            transactionsToTheMoon,
            tf.full_transactions
        ], ignore_index=True)


# the main running part of this python application
if __name__ == '__main__':
    # keep track of staring time
    start = time.perf_counter()

    # initialize Queue for inter thread communication
    q = mp.Queue()

    # extract files
    files = glob.glob("C:/Users/janieland/Documents/Development/Python/CodeOptimization/database/output/receiver*")
    nFiles = files.__len__()

    processes = []

    print("spinning processes up")

    # initialize the processes
    p1 = mp.Process(target=filterReceiverMoon, args=(files[0:250], q))
    p2 = mp.Process(target=filterReceiverMoon, args=(files[251:500], q))
    p3 = mp.Process(target=filterReceiverMoon, args=(files[501:750], q))
    p4 = mp.Process(target=filterReceiverMoon, args=(files[751:1000], q))
    pf = mp.Process(target=mergeSenderMoon, args=(q,))

    p1.start()
    p2.start()
    p3.start()
    p4.start()
    pf.start()

    q.close()
    q.join_thread()

    p1.join()
    p2.join()
    p3.join()
    p4.join()
    pf.join()

    # keep track of finishing time
    finish = time.perf_counter()

    print(f"Executed in: {(finish-start)}")

    exit(-1)

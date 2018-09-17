from multiprocessing import Process, Queue
import pandas as pd
import time
import glob

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
        # read the file
        receiver_leg = pd.read_csv(filepath_receiver, sep=';')

        # check if there are any transactions from The Moon
        transaction_in_scope = receiver_leg.receiverCountryISO == 'TMN'

        # check if there are any transactions left
        if sum(transaction_in_scope) > 0:
            # At least one transaction to the moon was found!
            filepath_send = filepath_receiver.replace("receiver", "sender")

            # let's quickly let the next worker know what file to read
            q.put(filepath_send)

            # now let's attach the data we got already from this leg
            transactions_rec = receiver_leg[transaction_in_scope]
            q.put(transactions_rec)

    # put a marker on the queue to let the next step know: nothing else is coming
    q.put("/0/")


def mergeSenderMoon(q):
    """
    Reads the sender leg of a transaction from the queue and merges it to the receiver part

    :param q:   The data input queue
    :return:
    """

    # initialize empty data frame
    transactionsToTheMoon = pd.DataFrame()

    # keep looping until the loop is broken out of
    while True:
        # get the file path from the queue
        filepath_send = q.get()

        # check if this is not the "we're done" path
        if filepath_send == "/0/":
            break

        # read the sender leg
        sender_leg = pd.read_csv(filepath_send, sep=";")

        # grab the receiver leg from the queue
        receiver_leg = q.get()

        # merge the sending and receiving part
        transactionsToTheMoon = pd.concat([
            transactionsToTheMoon,
            pd.merge(receiver_leg, sender_leg, on="transactionID", how="left")
        ], ignore_index=True)


# the main running part of this python application
if __name__ == '__main__':
    # keep track of staring time
    start = time.perf_counter()
    # initialize Queue for inter thread communication
    q = Queue()

    # extract files
    files = glob.glob("C:/Users/janieland/Documents/Development/Python/CodeOptimization/database/output/receiver*")

    # initialize the processes
    process_1 = Process(target=filterReceiverMoon, args=(files, q))
    process_2 = Process(target=mergeSenderMoon, args=(q, ))
    # start the processes
    process_1.start()
    process_2.start()

    # close the queueu
    q.close()
    q.join_thread()

    # join the threads
    process_1.join()
    process_2.join()

    # keep track of finishing time
    finish = time.perf_counter()

    print(f"Executed in: {(finish-start)}")

    exit(-1)

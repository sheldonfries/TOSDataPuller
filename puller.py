import tosdb
import csv
import threading
import time
from config import *
tosdb.init(root=dir)
tosdb.set_block_limit(20)
block1 = tosdb.TOSDB_DataBlock(10000, True)
block1.add_topics(topics_for_items[0], topics_for_items[1], topics_for_items[2])
block1.add_items(items[0], items[1], items[2])

topics = [block1.items()[2], block1.items()[1], block1.items()[0]]

vol_changes = [0, 0, 0]
price_changes = [0, 0, 0]
last_vols = [0, 0 ,0]
last_prices = [0, 0, 0]

data_SPY = []
data_GOOG = []
data_AAPL = []

def printResults(num):
    if num == 0:
        with open('outputSPY.csv', 'a') as f1:
            writer = csv.writer(f1, delimiter=',', lineterminator='\n')
            writer.writerow(data_SPY)
    elif num == 1:
        with open('outputGOOG.csv', 'a') as f2:
            writer = csv.writer(f2, delimiter=',', lineterminator='\n')
            writer.writerow(data_GOOG)
    elif num == 2:
        with open('outputAAPL.csv', 'a') as f3:
            writer = csv.writer(f3, delimiter=',', lineterminator='\n')
            writer.writerow(data_AAPL)

while True:
    topic_frames = [block1.topic_frame(topics[0], date_time=True, labels=True), block1.topic_frame(topics[1], date_time=True, labels=True), block1.topic_frame(topics[2], date_time=True, labels=True)]
    vols = [topic_frames[0].CUSTOM1[0], topic_frames[1].CUSTOM1[0], topic_frames[2].CUSTOM1[0]]
    prices = [topic_frames[0].LAST[0], topic_frames[1].LAST[0], topic_frames[2].LAST[0]]
    times = [topic_frames[0].LAST[1], topic_frames[1].LAST[1], topic_frames[2].LAST[1]]
    for x in range(len(topics)):
        if(vols[x] != '' and vols[x] != 'loading'):
            vol_changes[x] = float(vols[x]) - float(last_vols[x])
            price_changes[x] = float(prices[x]) - float(last_prices[x])
            if vol_changes[x] != 0:
                temp = topics[x] + ": Volume Change - " + str(vol_changes[x]) + " Price Change - " + str(price_changes[x])
                print(temp)
                last_vols[x] = vols[x]
                last_prices[x] = prices[x]
                if x == 0:
                    data_SPY = [times[x], vol_changes[x], price_changes[x]]
                elif x == 1:
                    data_GOOG = [times[x], vol_changes[x], price_changes[x]]
                elif x == 2:
                    data_AAPL = [times[x], vol_changes[x], price_changes[x]]
                printResults(x)
                time.sleep(1)
    
tosdb.clean_up()
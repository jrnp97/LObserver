import threading
import time
import multiprocessing
from multiprocessing import JoinableQueue as PQueue  # Process Queue
from queue import Queue  # Thread Queue

tqueue = Queue()
pqueue = PQueue()


def pworker():
    while True:
        process = pqueue.get()
        with open('test.txt', 'w') as f:
            f.write(str(process))
        pqueue.task_done()


def tworker():
    while True:
        process = tqueue.get()
        pqueue.put(process)
        tqueue.task_done()


# turn-on the worker thread
threading.Thread(target=tworker, daemon=True).start()
threading.Thread(target=tworker, daemon=True).start()
multiprocessing.Process(target=pworker, daemon=True).start()

# send thirty task requests to the worker
for item in range(500):
    tqueue.put(item)
print('All task requests sent\n', end='')
# for item in range(500):
#     pqueue.put(item)
print('PAll task requests sent\n', end='')


# block until all tasks are done
tqueue.join()
pqueue.join()
print('All work completed')

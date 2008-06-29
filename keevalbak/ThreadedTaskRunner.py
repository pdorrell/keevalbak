import Queue
import threading

class TaskProcessor(threading.Thread):
    def __init__(self, queue, index):
        threading.Thread.__init__(self)
        self.queue = queue
        self.index = index

    def run(self):
        while True:
            task = self.queue.get()
            print "Thread %d performing task ..." % self.index
            task.doUnsynchronized()
            self.queue.task_done()
            print "Thread %d finished performing task ..." % self.index
            
class TaskRunner:
    def __init__(self, numThreads = 10):
        self.queue = Queue.Queue()
        for i in range(numThreads):
            t = TaskProcessor(self.queue, i)
            t.setDaemon(True)
            t.start()
                
    def runTasks(self, tasks):
        for task in tasks:
            self.queue.put (task)
        self.queue.join()
        for task in tasks:
            task.doSynchronized()

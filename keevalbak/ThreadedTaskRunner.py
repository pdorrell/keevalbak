import Queue
import threading

class TaskProcessor(threading.Thread):
    def __init__(self, queue, index, clonedAttributes):
        threading.Thread.__init__(self)
        self.queue = queue
        self.index = index
        self.clonedAttributes = clonedAttributes
        self.clonedAttributesMap = {}

    def run(self):
        while True:
            task = self.queue.get()
            print "Thread %d performing task ..." % self.index
            for attribute in self.clonedAttributes:
                if not self.clonedAttributesMap.has_key (attribute):
                    attributeValue = task.__dict__[attribute].clone()
                    self.clonedAttributesMap[attribute] = attributeValue
                task.__dict__[attribute] = self.clonedAttributesMap[attribute]
            task.doUnsynchronized()
            self.queue.task_done()
            print "Thread %d finished performing task ..." % self.index
            
class ThreadedTaskRunner:
    def __init__(self, numThreads = 10, clonedAttributes = []):
        self.queue = Queue.Queue()
        self.processors = [TaskProcessor(self.queue, i, clonedAttributes) for i in range(numThreads)]
        for processor in self.processors:
            processor.setDaemon(True)
            processor.start()
                
    def runTasks(self, tasks, checkpointTask = None):
        for task in tasks:
            self.queue.put (task)
        self.queue.join()
        for task in tasks:
            task.doSynchronized()

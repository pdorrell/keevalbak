# Copyright (c) 2008 Philip Dorrell, http://www.1729.com/
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

import Queue
import threading

class TaskRunner(object):
    """Simple task runner: runs both parts of tasks synchronously"""
    def __init__(self, checkpointFreq = None):
        self.checkpointFreq = checkpointFreq
        
    def runTasksInit(self):
        pass
    
    def doUnsynchronizedTasks(self, tasks):
        for task in tasks:
            task.doUnsynchronized()
        
    def runTasks(self, tasks, checkpointTask = None):
        self.runTasksInit()
        startIndex = 0
        numTasks = len(tasks)
        while startIndex < numTasks:
            if self.checkpointFreq == None or checkpointTask == None:
                endIndex = numTasks
            else:
                endIndex = min(startIndex+self.checkpointFreq, numTasks)
            self.doUnsynchronizedTasks (tasks[startIndex:endIndex])
            for i in range(startIndex, endIndex):
                tasks[i].doSynchronized()
            if endIndex < numTasks:
                print "CHECKPOINT:"
                if checkpointTask != None:
                    checkpointTask.checkpoint()
            startIndex = endIndex
            
class TaskProcessor(threading.Thread):
    def __init__(self, queue, index):
        threading.Thread.__init__(self)
        self.queue = queue
        self.index = index
        self.threadLocals = None

    def run(self):
        while True:
            task = self.queue.get()
            if self.threadLocals == None:
                self.threadLocals = task.getThreadLocals()
            #print "Thread %d performing task ..." % self.index
            for key, value in self.threadLocals.iteritems():
                task.__dict__[key] = value
            task.doUnsynchronized()
            self.queue.task_done()
            #print "Thread %d finished performing task ..." % self.index
            
class ThreadedTaskRunner(TaskRunner):
    def __init__(self, checkpointFreq = 10, numThreads = 10):
        super(ThreadedTaskRunner, self).__init__(checkpointFreq)
        self.queue = Queue.Queue()
        self.processors = [TaskProcessor(self.queue, i) for i in range(numThreads)]
        for processor in self.processors:
            processor.setDaemon(True)
            processor.start()
            
    def runTasksInit(self):
        for processor in self.processors:
            processor.threadLocals = None
                
    def doUnsynchronizedTasks(self, tasks):
        for task in tasks:
            self.queue.put (task)
        self.queue.join()

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
            #print "Thread %d performing task ..." % self.index
            for attribute in self.clonedAttributes:
                if not self.clonedAttributesMap.has_key (attribute):
                    attributeValue = task.__dict__[attribute].clone()
                    self.clonedAttributesMap[attribute] = attributeValue
                task.__dict__[attribute] = self.clonedAttributesMap[attribute]
            task.doUnsynchronized()
            self.queue.task_done()
            #print "Thread %d finished performing task ..." % self.index
            
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

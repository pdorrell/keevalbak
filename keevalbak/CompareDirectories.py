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

import os, sys

class DirectoryComparator:
    def __init__(self, base1, base2, log, logDiff):
        """An intention to compare all files within two base directories, 
        with specified logger (for progress messages) and difference logger"""
        self.base1 = base1
        self.base2 = base2
        self.log = log
        self.logDiff = logDiff

    def compareDirs(self, subPath = None, indent = 0):
        """Recursively compared the specified sub-directory in each base directory"""
        dir1 = subPath and os.path.join(self.base1, subPath) or self.base1
        dir2 = subPath and os.path.join(self.base2, subPath) or self.base2
        self.log(indent, "comparing directories %s and %s ..." % (dir1, dir2))
        dir1ChildrenSet = set()
        for name1 in os.listdir(dir1):
            dir1ChildrenSet.add (name1)
            child1Path = os.path.join(dir1, name1)
            child2Path = os.path.join(dir2, name1)
            childSubPath = subPath and ("%s/%s" % (subPath, name1)) or name1
            if os.path.isdir(child1Path):
                if os.path.exists(child2Path):
                    if os.path.isfile(child2Path):
                        self.logDiff ("%s is a directory in %s but a file in %s" % 
                                      (childSubPath, self.base1, self.base2))
                    elif os.path.isdir(child2Path):
                        self.compareDirs(childSubPath, indent = indent+1)
                    else:
                        self.logDiff("Unknown object %s in %s" % (childSubPath, self.base2))
                else:
                    self.logDiff("%s is a directory in %s but does not exist in %s" % 
                                 (childSubPath, self.base1, self.base2))
            elif os.path.isfile(child1Path):
                if os.path.exists(child2Path):
                    if os.path.isdir(child2Path):
                        self.logDiff("%s is a file in %s but a directory in %s" % 
                                     (childSubPath, self.base1, self.base2))
                    elif os.path.isfile(child2Path):
                        self.compareFiles(subPath = childSubPath, indent = indent+1)
                    else:
                        self.logDiff("Unknown object %s in %s" % (childSubPath, self.base2))
                else:
                    self.logDiff("%s is a file in %s but does not exist in %s" % 
                                 (childSubPath, self.base1, self.base2))
            else:
                self.logDiff("Unknown object %s in %s" % (childSubPath, self.base1))
        for name2 in os.listdir(dir2):
            if not name2 in dir1ChildrenSet:
                child1Path = os.path.join(dir1, name2)
                child2Path = os.path.join(dir2, name2)
                childSubPath = subPath and ("%s/%s" % (subPath, name2)) or name2
                if os.path.isdir(child2Path):
                    self.logDiff("%s does not exist in %s but is a directory in %s" % 
                                 (childSubPath, self.base1, self.base2))
                elif os.path.isfile(child2Path):
                    self.logDiff("%s does not exist in %s but is a file in %s" % 
                                 (childSubPath, self.base1, self.base2))
                else:
                    self.logDiff("Unknown object %s in %s" % (childSubPath, self.base2))
                    
    def compareFiles(self, indent, subPath):
        """Compare the specified file within each base directory"""
        file1 = os.path.join(self.base1, subPath)
        file2 = os.path.join(self.base2, subPath)
        self.log(indent, "comparing files %s and %s ..." % (file1, file2))
        contents1 = file(file1, "rb").read()
        contents2 = file(file2, "rb").read()
        if contents1 != contents2:
            self.logDiff("File %s has different contents in %s and %s" % (subPath, self.base1, self.base2))
            
def printLog(indent, message):
    """Simple implementation for progress logger"""
    print ("  " * indent) + message
    
class ErrorDiff:
    """Logger for comparison differences"""
    def __init__(self):
        self.errors = []
    def __call__(self, message):
        """Print error message when a difference is found.
        Also accumulate the errors, so you can check at the end
        if there were any."""
        print "##ERROR: %s" % message
        self.errors.append (message)

def verifyIdentical(dir1, dir2):
    """Verify two directories have identical sub-directories and file contents.
    Raise an error if there is a difference.
    Note: any meta-data such as protections or create/modified dates are currently ignored."""
    errorDiff = ErrorDiff()
    comparison = DirectoryComparator(dir1, dir2, 
                                     log = printLog, logDiff = errorDiff)
    comparison.compareDirs()
    print "Errors = %r" % errorDiff.errors
    numErrors = len(errorDiff.errors)
    if numErrors > 0:
        raise "%d differences found between %s and %s: %s" % (numErrors, 
                                                              dir1, dir2, 
                                                              ", ".join(errorDiff.errors))

def main():
    """Compare two directories passed in as arguments"""
    args = sys.argv[1:]
    if len(args) != 2:
        raise Exception("Useage: %s dir1 dir2" % sys.argv[0])
    verifyIdentical(args[0], args[1])

if __name__ == '__main__':
    main()

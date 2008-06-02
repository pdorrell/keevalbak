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

from s3bucketmap import S3BucketMap
import sys

def testContains(bucketMap, key):
    print "%s in bucket = %r" % (key, key in bucketMap)
    
def makeBucketMapFromSysArgvOrLocalEnv(prefix = ""):
    args = sys.argv[1:]
    if len(args) == 0:
        print "Looking for bucket args in module localenv ..."
        import localenv.s3
        accessKey = localenv.s3.accessKey
        secretAccessKey = localenv.s3.secretAccessKey
        bucketName = localenv.s3.testBucket
    elif len(args) != 3:
        raise Exception("Useage: %s accessKey secretAccessKey bucketName" % sys.argv[0])
    else:
        accessKey, secretAccessKey, bucketName = args[0], args[1], args[2]
    return S3BucketMap(accessKey, secretAccessKey, bucketName, prefix)
    
def main():
    bucketMap = makeBucketMapFromSysArgvOrLocalEnv("testPrefix/")
    print "%s" % bucketMap
    bucketMap["testValue"] = "fred"
    del bucketMap["junkvalue"]
    print "testValue = %r" % bucketMap["testValue"]
    del bucketMap["testValue"]
    #print "testValue = %r" % bucketMap["testValue"]
    bucketMap["testValue"] = "fred2"
    bucketMap["testValue2"] = "jim"
    
    print "testValue = %r" % bucketMap["testValue"]

    for key in bucketMap:
        print "key = %s" % key

    testContains(bucketMap, "jim")
    testContains(bucketMap, "testValue")

if __name__ == '__main__':
    main()

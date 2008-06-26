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

from boto.s3.connection import S3Connection
from boto.s3.bucketlistresultset import BucketListResultSet
from boto.s3.key import Key


class BaseS3BucketMap(object):
    def __init__(self, s3Connection, bucketName, prefix = ""):
        self.s3Connection = s3Connection
        self.bucket = self.s3Connection.get_bucket(bucketName)
        self.bucketName = bucketName
        self.prefix = prefix
        
    def bucketKey(self, key):
        return unicode(self.prefix + key).encode('utf-8')
        
    def subMap(self, prefix):
        return BaseS3BucketMap(self.s3Connection, self.bucketName, self.prefix + prefix)
        
    def __getitem__(self, key):
        valueKey = self.bucket.lookup(self.bucketKey(key))
        if valueKey is None: 
            raise KeyError(u"%s" % key)
        return valueKey.get_contents_as_string()
    
    def __contains__(self, key):
        return self.bucket.lookup(self.bucketKey(key)) is not None
    
    def __setitem__(self, key, value):
        valueKey = Key(self.bucket)
        valueKey.name = self.bucketKey(key)
        if not isinstance(value, str):
            raise TypeError('Cannot store non-string value')
        valueKey.set_contents_from_string(value)
    
    def __delitem__(self, key):
        # this does not return any KeyError if the key doesn't exist
        # (and it would cost more to check, so it doesn't check)
        self.bucket.delete_key(self.bucketKey(key))

    def __iter__(self):
        for s3Key in BucketListResultSet(self.bucket, prefix = self.prefix):
            s3KeyString = str(s3Key.key)
            if s3KeyString.startswith(self.prefix): # probably this check is unnecessary
                yield s3KeyString[len(self.prefix):]

    def __repr__(self):
        return "<S3BucketMap, bucket:%s, prefix = \"%s\">" % (self.bucketName, self.prefix)

    def __str__(self):
        return self.__repr__()

class S3BucketMap(BaseS3BucketMap):
    """Simplest possible implementation of Python map methods using an Amazon S3 bucket, 
    and optionally a key.
    
    For example, with a bucket name "bucket1" and prefix "myprefix", the key "mykey" is represented
    by the key "myprefixmykey" in bucket "bucket1".
    
    The implementation only stores values which are byte strings.
    """
    
    def __init__(self, accessKey, secretAccessKey, bucketName, prefix = "", secure = True):
        """Initialize using standard S3 bucket details and optional prefix"""
        s3Connection = S3Connection(accessKey, secretAccessKey, secure)
        super(S3BucketMap, self).__init__(s3Connection, bucketName, prefix)

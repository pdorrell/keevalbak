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

import BackupOperations
from s3bucketmap import S3BucketMap

# You need to define a localenv module that includes the required data ...
# Includes definition of "named" backups, each one specifying a source directory
# and a destination prefix key within the backup bucket.
import localenv.s3
import localenv.backups

testRestoreDir = localenv.backups.testRestoreDir
print "testRestoreDir = %s" % testRestoreDir

def getBackupMap(backupName):
    """Get the backup map for the named backup"""
    backupPrefix = localenv.backups.backups[backupName].prefix
    return S3BucketMap(localenv.s3.accessKey, localenv.s3.secretAccessKey, 
                       localenv.backups.backupBucket, prefix = backupPrefix)

def backup(backupName, full, verify, verifyIncrementally = False, doTheBackup = True):
    """Do the named backup, with options for full (or incremental) and verify"""
    testRestoreDir = localenv.backups.testRestoreDir
    backupDetails = localenv.backups.backups[backupName]
    backupMap = getBackupMap(backupName)
    BackupOperations.doBackup (backupDetails.source, backupMap, testRestoreDir, full = full, 
                               verify = verify, verifyIncrementally = verifyIncrementally, 
                               doTheBackup = doTheBackup, 
                               recordTrigger = localenv.backups.recordTrigger)
    
def listBackups(backupName):
    """List all backups in the named backup"""
    backupMap = getBackupMap(backupName)
    BackupOperations.listBackups(backupMap)
    
def incrementalBackup(backupName, verify, verifyIncrementally = False):
    """Do an incremental backup"""
    backup(backupName, full = False, verify = verify, verifyIncrementally = verifyIncrementally)
    
def fullBackup(backupName, verify, doTheBackup = True):
    """Do a full backup"""
    backup(backupName, full = True, verify = verify, verifyIncrementally = False, doTheBackup = doTheBackup)
    
def pruneBackups(backupName, keep = 1, dryRun = True):
    """Prune backups from a named backup"""
    backupMap = getBackupMap(backupName)
    BackupOperations.pruneBackups(backupMap, keep = keep, dryRun = dryRun)

if __name__ == '__main__':
    # comment or uncomment lines here according to taste
    incrementalBackup("test", verify = True, verifyIncrementally = True)
    #incrementalBackup("test", verify = True)
    #fullBackup("test", doTheBackup = True, verify = True)
    #fullBackup("test", verify = False)
    #listBackups("test")
    #pruneBackups("test", keep = 2, dryRun = False)

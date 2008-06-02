from BackupOperations import doBackup
from s3bucketmap import S3BucketMap

# You need to define a localenv module that includes the required data ...
import localenv.s3
import localenv.backups

testRestoreDir = localenv.backups.testRestoreDir
print "testRestoreDir = %s" % testRestoreDir

def backup(backupName, full, verify):
    testRestoreDir = localenv.backups.testRestoreDir
    backupDetails = localenv.backups.backups[backupName]
    sourceDir, backupPrefix = backupDetails.source, backupDetails.prefix
    backupMap = S3BucketMap(localenv.s3.accessKey, localenv.s3.secretAccessKey, 
                            localenv.backups.backupBucket, prefix = backupPrefix)
    doBackup (sourceDir, backupMap, testRestoreDir, full = full, verify = verify)
    
def incrementalBackup(backupName, verify):
    backup(backupName, full = False, verify = verify)
    
def fullBackup(backupName, verify):
    backup(backupName, full = True, verify = verify)

if __name__ == '__main__':
    incrementalBackup("test", verify = True)
    #incrementalBackup("test", verify = False)
    #fullBackup("test", verify = True)
    #fullBackup("test", verify = False)

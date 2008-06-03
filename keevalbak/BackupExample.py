import BackupOperations
from s3bucketmap import S3BucketMap

# You need to define a localenv module that includes the required data ...
import localenv.s3
import localenv.backups

testRestoreDir = localenv.backups.testRestoreDir
print "testRestoreDir = %s" % testRestoreDir

def getBackupMap(backupName):
    backupPrefix = localenv.backups.backups[backupName].prefix
    return S3BucketMap(localenv.s3.accessKey, localenv.s3.secretAccessKey, 
                       localenv.backups.backupBucket, prefix = backupPrefix)

def backup(backupName, full, verify):
    testRestoreDir = localenv.backups.testRestoreDir
    backupDetails = localenv.backups.backups[backupName]
    backupMap = getBackupMap(backupName)
    BackupOperations.doBackup (backupDetails.source, backupMap, testRestoreDir, full = full, verify = verify)
    
def listBackups(backupName):
    backupMap = getBackupMap(backupName)
    BackupOperations.listBackups(backupMap)
    
def incrementalBackup(backupName, verify):
    backup(backupName, full = False, verify = verify)
    
def fullBackup(backupName, verify):
    backup(backupName, full = True, verify = verify)
    
def pruneBackups(backupName, keep = 1, dryRun = True):
    backupMap = getBackupMap(backupName)
    BackupOperations.pruneBackups(backupMap, keep = keep, dryRun = dryRun)

if __name__ == '__main__':
    incrementalBackup("test", verify = True)
    #incrementalBackup("test", verify = False)
    #fullBackup("test", verify = True)
    #fullBackup("test", verify = False)
    listBackups("test")
    pruneBackups("test", keep = 2, dryRun = True)

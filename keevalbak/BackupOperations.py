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

import yaml
import hashlib
import os
import time
import datetime
import shutil
import CompareDirectories
import re
from sets import Set

def readFileBytes(filename):
    """Read named file and return contents as a byte string"""
    f = file(filename, "rb")
    bytes = f.read()
    f.close()
    return bytes

def writeFileBytes(filename, bytes):
    """Write byte string as new contents of named file"""
    f = file(filename, "wb")
    f.write(bytes)
    f.close()

def deleteMapValues(map, dryRun):
    """Delete all keys from a map, or if dryRun is True, do a dry run"""
    print "%sDeleting keys from map %s" % (dryRun and "DRYRUN: " or "", map)
    for key in map:
        print " delete %r ..." % key
        if not dryRun:
            del map[key]
    print "finished."
    
class PathSummary(object):
    """Information about a file or directory specified as a relative path within some base directory
    Note: all paths are '/' separated, whether or not we are in Microsoft Windows"""
    def __init__(self, relativePath):
        self.relativePath = relativePath

    def fullPath(self, basePath):
        """Return the full path given the path of the base directory"""
        return basePath + self.relativePath

    @staticmethod
    def fromYamlData(data):
        """Convert YAML data into FileSummary or DirSummary (inverse of toYamlData methods)"""
        pathType = data["type"]
        if pathType == "file":
            return FileSummary.fromYamlData(data)
        elif pathType == "dir":
            return DirSummary.fromYamlData(data)
        else:
            raise "Unknown path type: %s" % pathType

class FileSummary(PathSummary):
    """Information about a file specified as a relative path within some (unspecified) base directory, 
    including a SHA1 hash of the file's contents."""
    def __init__(self, relativePath, hash, written = False):
        super(FileSummary, self).__init__(relativePath)
        self.isDir = False
        self.isFile = True
        self.hash = hash
        self.written = written
        
    def __unicode__(self):
        return u"FILE: %r : %s%s" % (self.relativePath, self.hash, self.written and " W" or "")
        
    def __repr__(self):
        return self.__unicode__()
    
    def toYamlData(self):
        """Convert to YAML"""
        return {"type": "file", 
                "path": self.relativePath, 
                "hash": self.hash, 
                "written": self.written
                }
    
    @staticmethod
    def fromYamlData(data):
        """Create from YAML (inverse of toYamlData)"""
        return FileSummary(data["path"], data["hash"], written = data["written"])

class DirSummary(PathSummary):
    """Information about a file specified as a relative path within some (unspecified) base directory"""
    def __init__(self, relativePath):
        super(DirSummary, self).__init__(relativePath)
        self.isDir = True
        self.isFile = False
        
    def __unicode__(self):
        return u"DIR:  %r" % (self.relativePath)
        
    def toYamlData(self):
        """Convert to YAML"""
        return {"type": "dir", 
                "path": self.relativePath
                }
    
    def __repr__(self):
        return self.__unicode__()
    
    @staticmethod
    def fromYamlData(data):
        """Create from YAML (inverse of toYamlData)"""
        return DirSummary(data["path"])
    
def sha1Digest(content):
    return hashlib.sha1(content).hexdigest()

class DirectoryInfo:
    """Information about all the directories and files within a base directory
       All directories are listed before any subdirectories or files contained within them.
    """
    def __init__(self, path):
        """Construct from path base directory"""
        self.path = unicode(path)
        self.pathSummaries = []
        self.summarizeSubDir(u"")
        
    def createDirSummary(self, relativePath):
        """Create a path summary for a sub-directory"""
        return DirSummary (relativePath)
    
    def createFileSummary(self, relativePath):
        """Create a path summary for a file in the base directory"""
        fileName = self.path + relativePath
        content = readFileBytes(fileName)
        fileHash = sha1Digest(content)
        return FileSummary (relativePath, fileHash)
    
    def addSummary(self, pathSummary):
        """Add a path summary"""
        print u"%r" % pathSummary
        self.pathSummaries.append (pathSummary)
        
    def getPathSummariesYamlData(self):
        """Return array of path summaries as YAML data"""
        return [summary.toYamlData() for summary in self.pathSummaries]
    
    def summarizeSubDir(self, relativePath):
        """Recursively summarize a sub-directory specified by it's relative path, 
        adding the path summaries for all contained files and sub-directories to the list of path summaries."""
        for childName in os.listdir(self.path + relativePath):
            childRelativePath = relativePath + "/" + childName;
            childPath = self.path + childRelativePath
            if os.path.isfile(childPath):
                self.addSummary(self.createFileSummary(childRelativePath))
            elif os.path.isdir(childPath):
                self.addSummary(self.createDirSummary(childRelativePath))
                self.summarizeSubDir (childRelativePath)
            else:
                print "UNKNOWN OBJECT %r in %r" % (childName, self.path + relativePath)
                
class HashVerificationRecords(object):
    """Records of verified hashes of backed up files (i.e. verified by actually reading
    the file content out of the backup map and recalculating the hash).
    Note that this class is not yet used, and nothing is yet writing the verification records
    into the backup map."""
    
    def __init__(self, backupMap):
        self.backupMap = backupMap
        self.datetimeFileHashesMap = {}
        self.datetimeUpdated = Set()
        
    def getFileHashesMap(self, datetime):
        if datetime in self.datetimeFileHashesMap:
            fileHashesMap = self.datetimeFileHashesMap[datetime]
        else:
            fileHashesRecordFilename = datetime + "/verifiedFileHashes.yaml"
            if fileHashesRecordFilename in self.backupMap:
                fileHashesMap = yaml.safe_load(self.backupMap[fileHashesRecordFilename])
            else:
                fileHashesMap = {}
            self.datetimeFileHashesMap[datetime] = fileHashesMap
        return fileHashesMap
        
    def markVerified(self, datetime, filePath, contentHash):
        fileHashesMap = self.getFileHashesMap(datetime)
        fileHashesMap[filePath] = contentHash
        self.datetimeUpdated.add (datetime)
        
    def getWrittenFileHash(self, datetime, filePath):
        """Get the hash of a backed up file, either from an existing hash verification record, 
        or, read the file contents from the backup map and calculate the hash."""
        fileHashesMap = self.getFileHashesMap(datetime)
        if filePath in fileHashesMap:
            return fileHashesMap[filePath]
        else:
            content = self.backupMap[datetime + "/files" + filePath]
            contentHash = sha1Digest(content)
            self.markVerified(datetime, filePath, contentHash)
            return contentHash
        
    def updateRecords(self):
        """Update any newly verified hashes back into the backup map."""
        print "Verified hashes were updated for %r" % self.datetimeUpdated
        for datetime in self.datetimeUpdated:
            fileHashesRecordFilename = datetime + "/verifiedFileHashes.yaml"
            print "Updating verification records for %s = %s" % (datetime, 
                                                                 self.datetimeFileHashesMap[datetime])
            self.backupMap[fileHashesRecordFilename] = yaml.safe_dump (self.datetimeFileHashesMap[datetime])
            
class BackupRecord:
    """A record of a backup made: it's date/time, and whether it was full or incremental."""
    def __init__(self, type, datetime, completed):
        """construct from 'full' or 'incremental' and the date time"""
        self.type = type
        self.datetime = datetime
        self.completed = completed

    @staticmethod
    def fromYamlData(data):
        """Construct backup record from YAML data (inverse of toYamlData)"""
        # completed defaults to True because previous version of keevalback only recorded when complete
        return BackupRecord(data["type"], data["datetime"], data.get("completed", True))
        
    def toYamlData(self):
        """Convert to data to be stored in YAML"""
        return {"type": self.type, "datetime": self.datetime, "completed": self.completed}
    
    def isFull(self):
        return self.type == "full"
        
    def __str__(self):
        return "[Backup: %s %s %s]" % (self.type, self.datetime, self.completed and "complete" or "INCOMPLETE")
    
    def __repr__(self):
        return self.__str__()
    
class WrittenRecords:
    """Records of where file contents with a given SHA1 hash value was written to in backup map
    (within the context of a particular set of backups, i.e. a full and following incrementals)"""
    def __init__(self):
        self.written = {}
        
    def recordHashWritten(self, hash, key):
        """Record that a contents with a particular hash were written to a particular key"""
        print " record hash %s written to %r" % (hash, key)
        self.written[hash] = key
        
    def isWritten(self, hash):
        """Has a file contents with this hash value been written to the backup map?"""
        return hash in self.written
    
    def locationWritten(self, hash):
        """Where a file contents with this hash value was written to"""
        return self.written[hash]
    
    def recordBackup(self, backupMap, backupRecord):
        """For every file contents in a backup record recorded as written, record it's
        hash value and backup map key in the written records."""
        directoryInfoYamlData = yaml.safe_load (backupMap[backupRecord.datetime + "/pathList"])
        for pathData in directoryInfoYamlData:
            #print "Recording backup data %s/%r" % (backupRecord.datetime, pathData)
            if pathData["type"] == "file" and pathData["written"]:
                self.recordHashWritten (pathData["hash"], backupRecord.datetime + pathData["path"])
    
    def recordPreviousBackups(self, backupMap, backupRecords):
        """Record the hashes of all files written from the last full backup onwards (or from the first
        backup if for some reason there is no full backup."""
        fullFound = False
        i = len(backupRecords)-1
        while not fullFound and i >= 0:
            backupRecord = backupRecords[i]
            print "Recording backup %r ..." % backupRecord
            self.recordBackup(backupMap, backupRecord)
            if backupRecord.type == "full":
                fullFound = True
            i -= 1
            
class BaseFileHash(object):
    """Description of a file: it's (basic) name and hash"""
    def __init__(self, name, hash, description):
        self.name = name
        self.hash = hash
        self.description = description
        
    def isDir(self):
        return False
            
    def printIndented(self, indent):
        print "%sFile %r: %s" % (indent, self.name, self.hash)
        
    def compareToOtherFileHash (self, otherFileHash, indent, log, logDiff):
        if self.hash != otherFileHash.hash:
            self.logDiff ("File %r has hash %s in %r but hash %s in %r" %
                          self.name, self.hash, self.description, 
                          otherFileHash.hash, otherFileHash.description)
        
pathRegex = re.compile("[/]([^/]*)([/].*)?")
        
def analysePath(path):
    """Analyse a path starting with '/' and with '/' separators into 1st part and remainder
    e.g. '/x/y' into 'x' and '/y' and '/x' into 'x' and None."""
    pathMatch = pathRegex.match(path)
    rootPath = pathMatch.group(1)
    remainderPath = pathMatch.group(2)
    return (rootPath, remainderPath)

class BaseDirHash(object):
    """Description of a directory as a map of immediate sub-directories 
    and immediately contained files"""
    def __init__(self, name, description):
        self.name = name
        self.children = []
        self.childrenMap = {}
        self.description = description
        
    def isDir(self):
        return True
            
    def addChild(self, childHash):
        """Add a child, i.e. a directory or file"""
        self.children.append (childHash)
        self.childrenMap[childHash.name] = childHash
        
    def hasChildNamed(self, childName):
        return childName in self.childrenMap
        
    def printIndented(self, indent = ""):
        print "%sDir %r" % (indent, self.name)
        childIndent = "  " + indent
        for child in self.children:
            child.printIndented(indent = childIndent)
            
    def addFileSummary(self, path, hash):
        """Add a file given it's full path name relative to this directory
        (necessarily constructing the intermediate sub-directories if they
        are not already there)"""
        rootPath, remainderPath = analysePath(path)
        if remainderPath is None:
            self.addChild (BaseFileHash(rootPath, hash, self.description))
        else:
            childDirHash = self.getOrCreateChildDirHash(rootPath)
            childDirHash.addFileSummary (remainderPath, hash)
            
    def getOrCreateChildDirHash(self, name):
        """Return DirHash for an immediate sub-directory, creating it if necessary"""
        if name in self.childrenMap:
            return self.childrenMap[name]
        else:
            childDirHash = BaseDirHash(name, self.description)
            self.addChild(childDirHash)
            return childDirHash
            
    def addDirSummary(self, path):
        """Add a sub-directory given it's full path name relative to this directory
        (necessarily constructing the intermediate sub-directories if they
        are not already there)"""
        rootPath, remainderPath = analysePath(path)
        if remainderPath is None:
            self.addChild (BaseDirHash(rootPath, self.description))
        else:
            childDirHash = self.getOrCreateChildDirHash(rootPath)
            childDirHash.addDirSummary (remainderPath)
            
    def compareToOtherDirHash(self, otherDirHash, indent, log, logDiff):
        log (indent, "comparing directory %r" % self.name)
        for child1 in self.children:
            name1 = child1.name
            child2 = otherDirHash.childrenMap.get(name1, None)
            if child1.isDir():
                if child2 != None:
                    if not child2.isDir():
                        logDiff ("%r is a directory in %r but a file in %r" % 
                                 (name1, self.description, otherDirHash.description))
                    else:
                        child1.compareToOtherDirHash (child2, indent+1, log, logDiff)
                else:
                    logDiff("%r is a directory in %r but does not exist in %r" % 
                            (name1, self.description, otherDirHash.description))
            else:
                if child2 != None:
                    if child2.isDir():
                        logDiff("%r is a file in %r but a directory in %r" % 
                                (name1, self.description, otherDirHash.description))
                    else:
                        child1.compareToOtherFileHash (child2, indent+1, log, logDiff)
                else:
                    logDiff("%r is a file in %r but does not exist in %r" % 
                            (name1, self.description, otherDirHash.description))
            for child2 in otherDirHash.children:
                if not self.hasChildNamed (child2.name):
                    if child2.isDir():
                        logDiff("%r does not exist in %r but is a directory in %r" % 
                                (child2.name, self.description, otherDirHash.description))
                    else:
                        logDiff("%r does not exist in %r but is a file in %r" % 
                                (child2.name, self.description, otherDirHash.description))

class FileHash(BaseFileHash):
    """Information about a file with a relative path name based on actual
    contents of actual file in actual file-system base directory"""
    def __init__(self, dir, name, description):
        filename = dir + "/" + name
        content = readFileBytes (filename)
        super(FileHash, self).__init__(name, sha1Digest(content), description)
        
class DirHash(BaseDirHash):
    """Information about files within a directory with a relative path name 
    based on actual contents of actual directory in actual file-system base directory"""
    def __init__(self, dir, name, description):
        super(DirHash, self).__init__(name, description)
        fullPath = unicode (name and (dir + "/" + name) or dir)
        for childName in os.listdir(fullPath):
            childPath = fullPath + "/" + childName
            if os.path.isfile(childPath):
                self.addChild (FileHash(fullPath, childName, self.description))
            else:
                self.addChild (DirHash(fullPath, childName, self.description))
                
class ContentKey(object):
    def __init__(self, datetime, filePath):
        """Parameters for key used to look up file contents from a particular backup within a backup map.
        Note that filePath is expected to start with a '/'"""
        self.datetime = datetime
        self.filePath = filePath
        
    def fileKey(self):
        """The actual key.
        Note: "/files" infix is used to allow for other meta-data to be associated with the datetime."""
        return self.datetime + "/files" + self.filePath
    
    def __str__(self):
        return "[%s:%r]" % (self.datetime, self.filePath)
    
    def __repr__(self):
        return self.__str__()
    
class BackupRecordUpdater:
    """Object responsible for recording current state of backup in progress"""
    def __init__(self, backups, backupRecords, currentBackupRecord, backupKeyBase, 
                 directoryInfo, recordTrigger = 1000000):
        self.backups = backups
        self.backupRecords = backupRecords
        self.currentBackupRecord = currentBackupRecord
        self.backupKeyBase = backupKeyBase
        self.directoryInfo = directoryInfo
        self.bytesWritten = 0
        self.unrecordedBytes = 0
        self.recordTrigger = recordTrigger
        
    def checkpoint(self):
        self.backups.recordPathSummaries (self.backupKeyBase, self.directoryInfo)
        
    def record(self):
        self.checkpoint()
        self.backups.saveBackupRecords(self.backupRecords)
        
    def recordContentWrittenSize(self, contentWrittenSize):
        self.bytesWritten += contentWrittenSize
        print " wrote %d bytes, total now %d ..." % (contentWrittenSize, self.bytesWritten)
        self.unrecordedBytes += contentWrittenSize
        if self.unrecordedBytes >= self.recordTrigger:
            print " unrecordedBytes = %d, recording backup state ..." % self.unrecordedBytes
            self.checkpoint()
            self.unrecordedBytes = 0
        
    def recordCompleted(self):
        self.currentBackupRecord.completed = True
        self.record()
        
class TaskRunner:
    """Simple task runner: runs both parts of tasks synchronously"""
    def __init__(self, checkpointFreq = None):
        self.checkpointFreq = checkpointFreq
        
    def runTasks(self, tasks, checkpointTask = None):
        for task in tasks:
            task.doUnsynchronized()
        for task in tasks:
            task.doSynchronized()
        
class IncrementalBackups:
    """A set of dated full or incremental backups within a given backup map.
    This object does _not_ (currently) record _where_ the file contents came from.
    """
    def __init__(self, backupMap, recordTrigger = 10000000):
        self.backupMap = backupMap
        self.recordTrigger = recordTrigger
        
    def getDateTimeString(self):
        """Get a date time string to use for a new dated backup"""
        return time.strftime("%Y-%b-%d.%H-%M-%S")
    
    def getBackupRecords(self):
        """Retrieve the BackupRecord objects describing any existing backups"""
        if "backupRecords" in self.backupMap:
            backupsListYamlData = yaml.safe_load(self.backupMap["backupRecords"])
        else:
            backupsListYamlData = []
        return [BackupRecord.fromYamlData(record) for record in backupsListYamlData]
    
    def saveBackupRecords(self, backupRecords):
        backupRecordsYamlData = [record.toYamlData() for record in backupRecords]
        self.backupMap["backupRecords"] = yaml.safe_dump(backupRecordsYamlData)
        print "new backup records = %r" % backupRecords
    
    def getBackupGroups(self):
        """Get backup groups, i.e. backup records grouped into lists of incremental backups with a preceding
        full backup."""
        backupGroups = []
        records = self.getBackupRecords()
        currentBackupGroup = []
        for i, record in enumerate(records):
            if record.isFull() or i == 0:
                currentBackupGroup = [record]
                backupGroups.append (currentBackupGroup)
            else:
                currentBackupGroup.append(record)
        return backupGroups
    
    def listBackups(self):
        """Print out list of all backups"""
        recordGroups = self.getBackupGroups()
        for recordGroup in recordGroups:
            for i, record in enumerate(recordGroup):
                if i == 0:
                    indent = "*"
                else:
                    indent = "   "
                print "%s%s: %s %s" % (indent, record.type, record.datetime, 
                                       record.completed and "complete" or "INCOMPLETE")
                
    def pruneBackup(self, backupRecord, dryRun):
        """Prune the backup indicated by the backup record (with dry-run option)"""
        print "  prune backup %r" % backupRecord
        backupSubMap = self.backupMap.subMap(backupRecord.datetime)
        deleteMapValues(backupSubMap, dryRun)
                
    def pruneBackupGroup(self, recordGroup, dryRun):
        """Prune all backups in a backup group (with dry-run option)"""
        print "Backup group to prune: %r" % recordGroup
        for record in recordGroup:
            self.pruneBackup(record, dryRun)
                
    def pruneBackups(self, keep = 1, dryRun = True):
        """Prune previous backup groups, keeping only specified number of most
        recent backup groups (but at least one)"""
        print "Pruning backups, keep %d%s" % (keep, dryRun and ", DRY RUN" or "")
        if keep < 1:
            raise Exception ("Number of full backups to keep must be at least 1")
        recordGroups = self.getBackupGroups()
        if keep >= len(recordGroups):
            print "Only %d full backups, and %d specified to keep, so none will be pruned" % (len(recordGroups), keep)
        else:
            numToPrune = len(recordGroups) - keep
            groupsToPrune = recordGroups[:numToPrune]
            for recordGroup in groupsToPrune:
                self.pruneBackupGroup(recordGroup, dryRun = dryRun)
            if not dryRun:
                remainingGroups = recordGroups[numToPrune:]
                remainingRecords = []
                for group in remainingGroups:
                    remainingRecords += group
                self.saveBackupRecords(remainingRecords)
                
    def recordPathSummaries(self, backupKeyBase, directoryInfo):
        self.backupMap[backupKeyBase + "/pathList"] = yaml.safe_dump(directoryInfo.getPathSummariesYamlData())
        
    class BackupFileTask:
        def __init__(self, backupMap, backupFilesKeyBase, pathSummary, fileName, writtenRecords):
            self.backupMap = backupMap
            self.backupFilesKeyBase = backupFilesKeyBase
            self.pathSummary = pathSummary
            self.fileName = fileName
            self.writtenRecords = writtenRecords
            
        def doUnsynchronized(self):
            content = readFileBytes(self.fileName)
            self.fileContentKey = self.backupFilesKeyBase + self.pathSummary.relativePath
            print "Writing %r ..." % self.fileContentKey
            self.pathSummary.written = True
            self.backupMap[self.fileContentKey] = content
            
        def doSynchronized(self):
            self.writtenRecords.recordHashWritten (self.pathSummary.hash, self.fileContentKey)
            
    def doBackup(self, directoryInfo, full = True):
        """Create a new backup of a source directory (full or incremental).
        Note: 'incremental' is based on comparing the hashes of file contents already marked as
        written to previous backups in the same backup group. It is not based on any comparison
        of files done on the source computer. If a given file contents has already been written, 
        then the relevant file written as a pointer to the previous file with the same contents
        (which may or may not be the same file in the same place on the source computer).
        """
        dateTimeString = self.getDateTimeString()
        backupKeyBase = dateTimeString
        backupFilesKeyBase = backupKeyBase + "/files"
        print "retrieving existing backup records ..."
        backupRecords = self.getBackupRecords()
        print "backup records = %r" % backupRecords
        currentBackupRecord = BackupRecord(full and "full" or "incremental", dateTimeString, completed = False)
        backupRecords.append(currentBackupRecord)
        backupRecordUpdater = BackupRecordUpdater (self, backupRecords, currentBackupRecord, 
                                                   backupKeyBase, directoryInfo, recordTrigger = self.recordTrigger)
        backupRecordUpdater.record()
        writtenRecords = WrittenRecords()
        if not full:
            if len(backupRecords) == 0:
                full = True
                print "No previous records, so backup will be FULL anyway"
            else:
                writtenRecords.recordPreviousBackups (self.backupMap, backupRecords)
        backupFileTasks = []
        for pathSummary in directoryInfo.pathSummaries:
            if not pathSummary.isDir:
                fileName = pathSummary.fullPath(directoryInfo.path)
                if not writtenRecords.isWritten(pathSummary.hash):
                    backupFileTask = IncrementalBackups.BackupFileTask(self.backupMap, backupFilesKeyBase, 
                                                                       pathSummary, fileName, writtenRecords)
                    backupFileTasks.append (backupFileTask)
                else:
                    print "Content of %r already written to %r" % (pathSummary, 
                                                                   writtenRecords.locationWritten (pathSummary.hash))
        TaskRunner(checkpointFreq = 20).runTasks (backupFileTasks, checkpointTask = backupRecordUpdater)
        backupRecordUpdater.recordCompleted()
        
    def doFullBackup(self, directoryInfo):
        """Do a full backup of a source directory"""
        self.doBackup (directoryInfo, full = True)
        
    def doIncrementalBackup(self, directoryInfo):
        """Do an incremental backup of a source directory"""
        self.doBackup (directoryInfo, full = False)
        
    def getBackupRecordForDateTime(self, backupRecords, dateTimeString):
        for index, backupRecord in enumerate(backupRecords):
            if backupRecord.datetime == dateTimeString:
                return index
        raise "No backup record found for date-time %r" % dateTimeString
        
    def getRestoreRecords(self, backupRecords, dateTimeString):
        """Return records for the most recent backup group"""
        if dateTimeString is None:
            restorePos = len(backupRecords)-1
        else:
            restorePos = self.getBackupRecordForDateTime (backupRecords, dateTimeString)
        pos = restorePos
        while pos >= 0 and backupRecords[pos].type != "full":
            pos -= 1
        return backupRecords[pos:(restorePos+1)]
    
    def getPathSummaryDataList(self, backupRecord):
        """Get YAML data representing information about files and directories backed up
        in a specified dated backup"""
        dateTimeString = backupRecord.datetime
        backupKeyBase = dateTimeString
        print "getPathSummaryDataList for %r ..." % backupRecord
        pathSummariesData = yaml.safe_load(self.backupMap[backupKeyBase + "/pathList"])
        return pathSummariesData
    
    def getHashContentKeyMap(self, restoreRecords, pathSummaryLists):
        """Construct a map from hash keys to the backup keys to which those file contents
        were written (within the given backup group which is being restored from)"""
        hashContentKeyMap = {}
        for restoreRecord, pathSummaryList in zip(restoreRecords, pathSummaryLists):
            for pathSummary in pathSummaryList:
                if pathSummary.isFile and pathSummary.written:
                    hashContentKeyMap[pathSummary.hash] = ContentKey(restoreRecord.datetime, pathSummary.relativePath)
        return hashContentKeyMap
    
    class RestoreFileTask:
        def __init__(self, backupMap, contentKey, fullPath, updateVerificationRecords, verificationRecords, overwrite):
            self.backupMap = backupMap
            self.contentKey = contentKey
            self.fullPath = fullPath
            self.updateVerificationRecords = updateVerificationRecords
            self.verificationRecords = verificationRecords
            self.overwrite = overwrite
            
        def doUnsynchronized(self):
            content = self.backupMap[self.contentKey.fileKey()]
            if os.path.exists(self.fullPath) and self.overwrite:
                os.remove (self.fullPath)
            writeFileBytes(self.fullPath, content)
            if self.updateVerificationRecords:
                self.contentHash = sha1Digest(content)
            print "Restored FILE %r" % self.fullPath
                    
        def doSynchronized(self):
            if self.updateVerificationRecords:
                self.verificationRecords.markVerified (self.contentKey.datetime, 
                                                       self.contentKey.filePath, self.contentHash)
                print "Mark verified FILE %r" % self.fullPath
    
    def restoreDirectory(self, restoreDir, pathSummaryList, hashContentKeyMap, overwrite, 
                         updateVerificationRecords = False):
        """Restore a directory using path summaries and hash content key map, with optional overwrite"""
        print "Restoring directory %r ..." % restoreDir
        if updateVerificationRecords:
            verificationRecords = HashVerificationRecords(self.backupMap)
        restoreFileTasks = []
        for pathSummary in pathSummaryList:
            fullPath = pathSummary.fullPath (restoreDir)
            if pathSummary.isDir:
                if not os.path.isdir(fullPath):
                    os.makedirs(fullPath)
                print "Restored DIR  %r" % fullPath
            elif pathSummary.isFile:
                if not pathSummary.hash in hashContentKeyMap:
                    print "WARNING: No written content found for %r (hash %s)" % (pathSummary.relativePath, 
                                                                                  pathSummary.hash)
                contentKey = hashContentKeyMap[pathSummary.hash]
                restoreFileTasks.append (IncrementalBackups.RestoreFileTask (self.backupMap, contentKey, 
                                                                             fullPath, updateVerificationRecords, 
                                                                             verificationRecords, overwrite))
            else:
                print "WARNING: Unknown path type %r" % pathSummary
        TaskRunner().runTasks (restoreFileTasks)
        if updateVerificationRecords:
            verificationRecords.updateRecords()
            
    def getRestoreDetails(self, dateTimeString):
        backupRecords = self.getBackupRecords()
        print "backupRecords = %r" % backupRecords
        if len(backupRecords) == 0:
            raise "No backup records found"
        print "Get restore records for %s" % (dateTimeString or "(most recent backup)")
        restoreRecords = self.getRestoreRecords(backupRecords, dateTimeString)
        print "restoreRecords = %r" % restoreRecords
        pathSummaryDataLists = [self.getPathSummaryDataList(record) for record in restoreRecords]
        print "parsing pathSummaryLists from YAML data ..."
        pathSummaryLists = [[PathSummary.fromYamlData(pathSummaryData) for pathSummaryData in pathSummaryDataList] 
                            for pathSummaryDataList in pathSummaryDataLists]
        print "calculating hashContentKeyMap ..."
        hashContentKeyMap = self.getHashContentKeyMap(restoreRecords, pathSummaryLists)
        print "hashContentKeyMap = %r" % hashContentKeyMap
        backupToRestore = restoreRecords[-1]
        print "Target backup for restore: %r" % backupToRestore
        pathSummaryListToRestore = pathSummaryLists[-1]
        return pathSummaryListToRestore, hashContentKeyMap, backupToRestore
    
    def getRestoredDirHash(self, dateTimeString = None):
        pathSummaryList, hashContentKeyMap, backupToRestore = self.getRestoreDetails(dateTimeString)
        verificationRecords = HashVerificationRecords(self.backupMap)
        restoredDirHash = BaseDirHash(None, "backed up files")
        for pathSummary in pathSummaryList:
            if pathSummary.isDir:
                restoredDirHash.addDirSummary(pathSummary.relativePath)
                print " DIR  %r" % pathSummary.relativePath
            elif pathSummary.isFile:
                contentKey = hashContentKeyMap[pathSummary.hash]
                # We could compare pathSummary.hash and fileHash, 
                # but the verified fileHash is what matters (to compare to local file)
                fileHash = verificationRecords.getWrittenFileHash(contentKey.datetime, contentKey.filePath)
                restoredDirHash.addFileSummary(pathSummary.relativePath, fileHash)
                print " FILE %r" % pathSummary.relativePath
            else:
                print "WARNING: Unknown path type %r" % pathSummary
        verificationRecords.updateRecords()
        return restoredDirHash
        
    def incrementalVerify(self, sourceDir):
        """Incrementally verify a directory using path summaries and hash content key map, with optional overwrite"""
        print "Incrementally verifying against directory %r ..." % sourceDir
        restoredDirHash = self.getRestoredDirHash()
        print "RESTORE DIR HASH:"
        restoredDirHash.printIndented()
        print ""
        print "LOCAL DIR HASH for %r" % sourceDir
        localDirHash = DirHash(sourceDir, None, sourceDir)
        localDirHash.printIndented()
        errorDiff = CompareDirectories.ErrorDiff()
        localDirHash.compareToOtherDirHash (restoredDirHash, 0, CompareDirectories.printLog, errorDiff)
        errorDiff.logAndCheck (localDirHash.description, restoredDirHash.description)
            
    def restore(self, restoreDir, dateTimeString = None, 
                overwrite = False, updateVerificationRecords = False, allowIncomplete = False):
        """Restore the specified (or otherwise the most recent) backup to a 
        destination directory (with optional overwrite)"""
        if not os.path.exists(restoreDir):
            os.makedirs(restoreDir)
        if not os.path.isdir(restoreDir):
            raise "Restore target %r is not a directory" % restoreDir
        if not overwrite and len(os.listdir(restoreDir)) > 0:
            raise "Restore target %r is not empty" % restoreDir
        pathSummaryListToRestore, hashContentKeyMap, backupToRestore = self.getRestoreDetails(dateTimeString)
        if not allowIncomplete and not backupToRestore.completed:
            raise "Backup dated %s is not complete and allowIncomplete is set to false" % backupToRestore.datetime
        self.restoreDirectory (restoreDir, pathSummaryListToRestore, hashContentKeyMap, 
                               overwrite, updateVerificationRecords)
        print "Restored data to %r" % restoreDir
        
def listBackups(backupMap):
    """List all backups in a backup map"""
    IncrementalBackups(backupMap).listBackups()
        
def pruneBackups(backupMap, keep = 1, dryRun = True):
    """Prune backups in a backup map, keeping specified number of backup groups (minimum 1)"""
    IncrementalBackups(backupMap).pruneBackups(keep = keep, dryRun = dryRun)

def doBackup(sourceDirectory, backupMap, testRestoreDir = None, full = False, verify = False, 
             doTheBackup = True, verifyIncrementally = False, recordTrigger = 10000000):
    """Do a backup from source directory to backup map, with options 'full' (or incremental)
    and 'verify' (in which case a test restore is done to the test restore directory).
    Also, if 'doTheBackup' is set to false, only do the test restore and verify.
    """
    startTime = datetime.datetime.now()
    print ""
    print "Started %s" % startTime
    print ""
    if verify and testRestoreDir == None:
        raise "Must supply testRestoreDir argument if verify option is chosen"
    print "Backing up %r ..." % sourceDirectory
    backups = IncrementalBackups(backupMap, recordTrigger)
    srcDirInfo = DirectoryInfo(sourceDirectory)
    if doTheBackup:
        backups.doBackup (srcDirInfo, full = full)
        backupFinishedTime = datetime.datetime.now()
        backupFinishedMessage = "Backup finished %s (started %s)" % (backupFinishedTime, startTime)
        print ""
        print backupFinishedMessage
    restoreStartTime = datetime.datetime.now()
    if verify:
        print ""
        print "Verifying ..."
        if verifyIncrementally:
            print "   incrementally ..."
            backups.incrementalVerify (sourceDirectory)
        else:
            print "   fully ..."
            shutil.rmtree(testRestoreDir)
            backups.restore(testRestoreDir, overwrite = False, updateVerificationRecords = True)
            CompareDirectories.verifyIdentical(testRestoreDir, srcDirInfo.path)
        verifyFinishedTime = datetime.datetime.now()
        print ""
        if doTheBackup:
            print backupFinishedMessage
        print "Verify finished %s (started %s)" % (verifyFinishedTime, restoreStartTime)

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
    f = file(filename, "rb")
    bytes = f.read()
    f.close()
    return bytes

def writeFileBytes(filename, bytes):
    f = file(filename, "wb")
    f.write(bytes)
    f.close()

def deleteMapValues(map, dryRun):
    print "%sDeleting keys from map %s" % (dryRun and "DRYRUN: " or "", map)
    for key in map:
        print " delete %r ..." % key
        if not dryRun:
            del map[key]
    print "finished."
    
class PathSummary(object):
    def __init__(self, relativePath):
        self.relativePath = relativePath

    def fullPath(self, basePath):
        return basePath + self.relativePath

    def __repr__(self):
        return self.__str__()
    
    @staticmethod
    def fromYamlData(data):
        pathType = data["type"]
        if pathType == "file":
            return FileSummary.fromYamlData(data)
        elif pathType == "dir":
            return DirSummary.fromYamlData(data)
        else:
            raise "Unknown path type: %s" % pathType

class FileSummary(PathSummary):
    def __init__(self, relativePath, hash, written = False):
        super(FileSummary, self).__init__(relativePath)
        self.isDir = False
        self.isFile = True
        self.hash = hash
        self.written = written
        
    def __str__(self):
        return "FILE: %s : %s%s" % (self.relativePath, self.hash, self.written and " W" or "")
        
    def toYamlData(self):
        return {"type": "file", 
                "path": self.relativePath, 
                "hash": self.hash, 
                "written": self.written
                }
    
    @staticmethod
    def fromYamlData(data):
        return FileSummary(data["path"], data["hash"], written = data["written"])

class DirSummary(PathSummary):
    def __init__(self, relativePath):
        super(DirSummary, self).__init__(relativePath)
        self.isDir = True
        self.isFile = False
        
    def __str__(self):
        return "DIR:  %s" % (self.relativePath)
        
    def toYamlData(self):
        return {"type": "dir", 
                "path": self.relativePath
                }
    
    @staticmethod
    def fromYamlData(data):
        return DirSummary(data["path"])

class DirectoryInfo:
    def __init__(self, path):
        self.path = path
        self.pathSummaries = []
        self.summarizeSubDir("")
        
    def getDirSummary(self, relativePath):
        return DirSummary (relativePath)
    
    def getFileSummary(self, relativePath):
        fileName = self.path + relativePath
        content = readFileBytes(fileName)
        sha1Hash = hashlib.sha1(content).hexdigest()
        return FileSummary (relativePath, sha1Hash)
    
    def addSummary(self, pathSummary):
        print pathSummary
        self.pathSummaries.append (pathSummary)
        
    def getPathSummariesYamlData(self):
        return [summary.toYamlData() for summary in self.pathSummaries]
    
    def summarizeSubDir(self, relativePath):
        for childName in os.listdir(self.path + relativePath):
            childRelativePath = relativePath + "/" + childName;
            childPath = self.path + childRelativePath
            if os.path.isfile(childPath):
                self.addSummary(self.getFileSummary(childRelativePath))
            elif os.path.isdir(childPath):
                self.addSummary(self.getDirSummary(childRelativePath))
                self.summarizeSubDir (childRelativePath)
            else:
                print "UNKNOWN OBJECT %s in %s" % (childName, self.path + relativePath)
                
class HashVerificationRecords(object):
    def __init__(self, backupMap):
        self.backupMap = backupMap
        self.datetimeFileHashesMap = {}
        self.datetimeUpdated = Set()
        
    def getWrittenFileHash(self, datetime, filePath):
        if datetime not in self.datetimeFileHashesMap:
            self.datetimeFileHashesMap[datetime] = {}
        fileHashesMap = self.datetimeFileHashesMap[datetime]
        if datetime in self.datetimeFileHashesMap:
            fileHashesMap = self.datetimeFileHashesMap[datetime]
        else:
            fileHashesRecordFilename = datetime + "/fileHashes.yaml"
            if fileHashesRecordFilename in self.backupMap:
                fileHashesMap = yaml.safe_load(self.backupMap[fileHashesRecordFilename])
            else:
                fileHashesMap = {}
            self.datetimeFileHashesMap[datetime] = fileHashesMap
        if filePath in fileHashesMap:
            return fileHashesMap[filePath]
        else:
            content = self.backupMap[datetime + "/files" + filePath]
            fileHash = hashlib.sha1(content).hexdigest()
            fileHashesMap[filePath] = fileHash
            self.datetimeUpdated.add (datetime)
            return fileHash
        
    def updateVerificationRecords(self):
        for datetime in self.datetimeUpdated:
            fileHashesRecordFilename = datetime + "/fileHashes.yaml"
            print "Updating verification records for %s = %s" % (datetime, 
                                                                 self.datetimeFileHashesMap[datetime])
            self.backupMap[fileHashesRecordFilename] = yaml.dump (self.datetimeFileHashesMap[datetime])
            
class BackupRecord:
    def __init__(self, type, datetime):
        self.type = type
        self.datetime = datetime
        
    @staticmethod
    def fromYamlData(data):
        return BackupRecord(data["type"], data["datetime"])
        
    def toYamlData(self):
        return {"type": self.type, "datetime": self.datetime}
        
    def __str__(self):
        return "[Backup: %s %s]" % (self.type, self.datetime)
    
    def __repr__(self):
        return self.__str__()
    
class WrittenRecords:
    def __init__(self):
        self.written = {}
        
    def recordHashWritten(self, hash, key):
        print " record hash %s written to %s" % (hash, key)
        self.written[hash] = key
        
    def isWritten(self, hash):
        return hash in self.written
    
    def locationWritten(self, hash):
        return self.written[hash]
    
    def recordBackup(self, backupMap, backupRecord):
        directoryInfoYamlData = yaml.safe_load (backupMap[backupRecord.datetime + "/pathList"])
        for pathData in directoryInfoYamlData:
            print "Recording backup data %s/%r" % (backupRecord.datetime, pathData)
            if pathData["type"] == "file" and pathData["written"]:
                self.recordHashWritten (pathData["hash"], backupRecord.datetime + pathData["path"])
    
    def recordPreviousBackups(self, backupMap, backupRecords):
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
    def __init__(self, name, hash):
        self.name = name
        self.hash = hash
            
    def printIndented(self, indent):
        print "%sFile %s: %s" % (indent, self.name, self.hash)
    
pathRegex = re.compile("[/]([^/]*)([/].*)?")
        
def analysePath(path):
    pathMatch = pathRegex.match(path)
    rootPath = pathMatch.group(1)
    remainderPath = pathMatch.group(2)
    return (rootPath, remainderPath)

class BaseDirHash(object):
    def __init__(self, name):
        self.name = name
        self.children = []
        self.childrenMap = {}
        
    def addChild(self, childHash):
        self.children.append (childHash)
        self.childrenMap[childHash.name] = childHash
        
    def printIndented(self, indent = ""):
        print "%sDir %s" % (indent, self.name)
        childIndent = "  " + indent
        for child in self.children:
            child.printIndented(indent = childIndent)
            
    def addFileSummary(self, path, hash):
        rootPath, remainderPath = analysePath(path)
        if remainderPath is None:
            self.addChild (BaseFileHash(rootPath, hash))
        else:
            childDirHash = self.getOrCreateChildDirHash(rootPath)
            childDirHash.addFileSummary (remainderPath, hash)
            
    def getOrCreateChildDirHash(self, name):
        if name in self.childrenMap:
            return self.childrenMap[name]
        else:
            childDirHash = BaseDirHash(name)
            self.addChild(childDirHash)
            return childDirHash
            
    def addDirSummary(self, path):
        rootPath, remainderPath = analysePath(path)
        if remainderPath is None:
            self.addChild (BaseDirHash(rootPath))
        else:
            childDirHash = self.getOrCreateChildDirHash(rootPath)
            childDirHash.addDirSummary (remainderPath)

class FileHash(BaseFileHash):
    def __init__(self, dir, name):
        filename = dir + "/" + name
        content = readFileBytes (filename)
        super(FileHash, self).__init__(name, hashlib.sha1(content).hexdigest())
        self.isFile = True
        self.isDir = False
        
class DirHash(BaseDirHash):
    def __init__(self, dir, name = None):
        super(DirHash, self).__init__(name)
        fullPath = name and (dir + "/" + name) or dir
        for childName in os.listdir(fullPath):
            childPath = fullPath + "/" + childName
            if os.path.isfile(childPath):
                self.addChild (FileHash(fullPath, childName))
            else:
                self.addChild (DirHash(fullPath, childName))
                
class ContentKey(object):
    def __init__(self, datetime, filePath):
        self.datetime = datetime
        self.filePath = filePath
        
    def fileKey(self):
        return self.datetime + "/files" + self.filePath
            
class IncrementalBackups:
    def __init__(self, backupMap):
        self.backupMap = backupMap
        
    def getDateTimeString(self):
        return time.strftime("%Y-%b-%d.%H-%M-%S")
    
    def getBackupRecords(self):
        if "backupRecords" in self.backupMap:
            backupsListYamlData = yaml.safe_load(self.backupMap["backupRecords"])
        else:
            backupsListYamlData = []
        return [BackupRecord.fromYamlData(record) for record in backupsListYamlData]
        
    def doBackup(self, directoryInfo, full = True):
        dateTimeString = self.getDateTimeString()
        backupKeyBase = dateTimeString
        backupFilesKeyBase = backupKeyBase + "/files"
        backupRecords = self.getBackupRecords()
        print "backup records = %r" % backupRecords
        writtenRecords = WrittenRecords()
        if not full:
            if len(backupRecords) == 0:
                full = True
                print "No previous records, so backup will be FULL anyway"
            else:
                writtenRecords.recordPreviousBackups (self.backupMap, backupRecords)
        for pathSummary in directoryInfo.pathSummaries:
            if not pathSummary.isDir:
                fileName = pathSummary.fullPath(directoryInfo.path)
                if not writtenRecords.isWritten(pathSummary.hash):
                    content = readFileBytes(fileName)
                    fileContentKey = backupFilesKeyBase + pathSummary.relativePath
                    print "Writing %s ..." % fileContentKey
                    pathSummary.written = True
                    self.backupMap[fileContentKey] = content
                    writtenRecords.recordHashWritten (pathSummary.hash, fileContentKey)
                else:
                    print "Content of %r already written to %s" % (pathSummary, 
                                                                   writtenRecords.locationWritten (pathSummary.hash))
        self.backupMap[backupKeyBase + "/pathList"] = yaml.dump(directoryInfo.getPathSummariesYamlData())
        backupRecords.append(BackupRecord(full and "full" or "incremental", dateTimeString))
        backupRecordsYamlData = [record.toYamlData() for record in backupRecords]
        self.backupMap["backupRecords"] = yaml.dump(backupRecordsYamlData)
        print "new backup records = %r" % backupRecords
        
    def doFullBackup(self, directoryInfo):
        self.doBackup (directoryInfo, full = True)
        
    def doIncrementalBackup(self, directoryInfo):
        self.doBackup (directoryInfo, full = False)
        
    def getRestoreRecords(self, backupRecords):
        pos = len(backupRecords)-1
        while pos >= 0 and backupRecords[pos].type != "full":
            pos -= 1
        return backupRecords[pos:]
    
    def getPathSummaryDataList(self, backupRecord):
        dateTimeString = backupRecord.datetime
        backupKeyBase = dateTimeString
        pathSummariesData = yaml.safe_load(self.backupMap[backupKeyBase + "/pathList"])
        return pathSummariesData
    
    def getHashContentKeyMap(self, restoreRecords, pathSummaryLists):
        hashContentKeyMap = {}
        for restoreRecord, pathSummaryList in zip(restoreRecords, pathSummaryLists):
            for pathSummary in pathSummaryList:
                if pathSummary.isFile and pathSummary.written:
                    hashContentKeyMap[pathSummary.hash] = ContentKey(restoreRecord.datetime, pathSummary.relativePath)
        return hashContentKeyMap
    
    def restoreDirectory(self, restoreDir, pathSummaryList, hashContentKeyMap, overwrite):
        print "Restoring directory %s ..." % restoreDir
        for pathSummary in pathSummaryList:
            fullPath = pathSummary.fullPath (restoreDir)
            if pathSummary.isDir:
                if not os.path.isdir(fullPath):
                    os.makedirs(fullPath)
                print "Restored DIR  %s" % fullPath
            elif pathSummary.isFile:
                if not pathSummary.hash in hashContentKeyMap:
                    print "WARNING: No written content found for %s (hash %s)" % (pathSummary.relativePath, 
                                                                                  pathSummary.hash)
                content = self.backupMap[hashContentKeyMap[pathSummary.hash].fileKey()]
                if os.path.exists(fullPath) and overwrite:
                    os.remove (fullPath)
                writeFileBytes(fullPath, content)
                print "Restored FILE %s" % fullPath
            else:
                print "WARNING: Unknown path type %r" % pathSummary
        
    def restore(self, restoreDir, overwrite = False):
        if not os.path.exists(restoreDir):
            os.makedirs(restoreDir)
        if not os.path.isdir(restoreDir):
            raise "Restore target %s is not a directory" % restoreDir
        if not overwrite and len(os.listdir(restoreDir)) > 0:
            raise "Restore target %s is not empty" % restoreDir
        backupRecords = self.getBackupRecords()
        print "backupRecords = %r" % backupRecords
        if len(backupRecords) == 0:
            raise "No backup records found"
        restoreRecords = self.getRestoreRecords(backupRecords)
        print "restoreRecords = %r" % restoreRecords
        pathSummaryDataLists = [self.getPathSummaryDataList(record) for record in restoreRecords]
        pathSummaryLists = [[PathSummary.fromYamlData(pathSummaryData) for pathSummaryData in pathSummaryDataList] 
                            for pathSummaryDataList in pathSummaryDataLists]
        hashContentKeyMap = self.getHashContentKeyMap(restoreRecords, pathSummaryLists)
        print "hashContentKeyMap = %r" % hashContentKeyMap
        backupToRestore = restoreRecords[-1]
        print "Will restore %r" % backupToRestore
        print "pathSummaryLists = %r" % pathSummaryLists
        pathSummaryListToRestore = pathSummaryLists[-1]
        self.restoreDirectory (restoreDir, pathSummaryListToRestore, hashContentKeyMap, overwrite)
        print "Restored data to %s" % restoreDir
        
def doBackup(sourceDirectory, backupMap, testRestoreDir = None, full = False, verify = False, 
             doTheBackup = True):
    startTime = datetime.datetime.now()
    print ""
    print "Started %s" % startTime
    print ""
    if verify and testRestoreDir == None:
        raise "Must supply testRestoreDir argument if verify option is chosen"
    print "Backing up %s ..." % sourceDirectory
    backups = IncrementalBackups(backupMap)
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
        shutil.rmtree(testRestoreDir)
        backups.restore(testRestoreDir, overwrite = False)
        CompareDirectories.verifyIdentical(testRestoreDir, srcDirInfo.path)
        verifyFinishedTime = datetime.datetime.now()
        print ""
        if doTheBackup:
            print backupFinishedMessage
        print "Verify finished %s (started %s)" % (restoreStartTime, backupFinishedTime)

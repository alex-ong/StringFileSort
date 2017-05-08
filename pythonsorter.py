import string
import sys
from multiprocessing import Pool
import os
import time
import shutil

choices = string.ascii_uppercase

import prettyprint


def sort_file_one_arg(arg):
    fileName,start_byte,num_bytes,procID = arg
    return  sort_file(fileName,start_byte,num_bytes,procID)
    
#override this function with something if you want it to be processed
def process_string(input):
    #time.sleep(0.001)
    return input
    
def calcPartitions(fileName, num_bytes):
    result = []
    byteCounter = 0
    filesize = os.stat(fileName).st_size
    result.append(byteCounter)
    
    with open(fileName, 'r') as f:
        while byteCounter < filesize:
            byteCounter += num_bytes            
            f.seek(byteCounter)
            char = f.read(1)        
            while char != '\n' and char != '':
                byteCounter += 1
                char = f.read(1)
            result.append(byteCounter)
            
    return result
    
def sort_file(fileName,start_byte,num_bytes,procID):
    real_start_byte = 0
    real_end_byte = start_byte+num_bytes
    
    with open(fileName, 'r') as f:
        f.seek(start_byte)        
        tempStr = f.read(num_bytes)
        while not tempStr.endswith('\n'):
            char = f.read(1)
            real_end_byte += 1
            if char == "":
                break
            tempStr += char

        #cull first option unless it is perfect
        cullFirst = True
        if start_byte != 0:
            f.seek(start_byte-1)
            firstChar = f.read(1)
            if firstChar == '\n':
                cullFirst = False
        else: #start_byte == 0
            cullFirst = False
            
    allEntries = tempStr.split('\n')
    
    real_start_byte = start_byte
    if cullFirst:
        real_start_byte += len(allEntries[0])
        allEntries = allEntries[1:]
        
        
    files = {}
    for char in choices:
        f = open(getFile(char,procID), 'a+')
        files[char] = f
        
    for entry in allEntries:        
        if len(entry) > 0 and len(entry.strip()) > 0:
            newEntry = process_string(entry).strip()
            files[newEntry[0]].write(newEntry + '\n')        
        
    for file in files.values():
        file.close()
        
    return ([real_start_byte,real_end_byte], [start_byte, start_byte+num_bytes])
        
def sort_completed_one_arg(args):
    procID, char = args
    sort_completed(procID, char)
    
def sort_completed(procID, char):     
    try:
        f = open(getFile(char,procID), 'r')
    except FileNotFoundError: #we didnt create a file
        return
        
    lines = f.readlines()
    lines.sort()
    f.close()
    f = open(getFile(char,procID), 'w')
    f.writelines(lines)
    
def getFile(char,procID):
    return char + str(procID) + '.txt'
    
def merge_sorted(char, numProcs):
    files = []
    for i in range (numProcs):
        try:
            f = open(getFile(char,i))
            files.append(f)
        except FileNotFoundError: #file wasn't created
            pass
            
        
    
    final = open(getFile(char,'_final'),'w')
    
    currentCmp = []
    for file in files:
        currentCmp.append(file.readline().strip())
        
    # store a pointer to each file. add smallest element of the four files
    # then push pointer forward. Do this till files are exhausted.
    while len(files) > 0:        
        minimum = min(currentCmp) #grab smallest string
        
        index = currentCmp.index(minimum)
        if len(minimum) != 0:
            final.write(minimum + '\n') #write out string        
        
        # increment string pointers
        newValue = files[index].readline()
        if not newValue:
            files[index].close()
            del files[index]
            del currentCmp[index]
        else:
            currentCmp[index] = newValue.strip()
    # delete existing files
    for fileName in (getFile(char, i) for i in range (numProcs)):
        try:
            os.remove(fileName)
        except:
            pass
        
def merge_final(char):
    final = open('sorted.blob','a+')
    merger = open(getFile(char,'_final'),'r')
    
    shutil.copyfileobj(merger, final)    
    
    final.close()
    merger.close()
    os.remove(getFile(char,'_final'))
    
if __name__ == '__main__':
    if len(sys.argv) < 3:
        print ("usage: pythonsorter.py blob_file THREADS")
        sys.exit()
        
    fileName = sys.argv[1]
    threads = int(sys.argv[2])
    
    p = Pool(threads)
    blockSize = 1024*1024*50 #50 megabytes
    fileName = 'rawblob.blob'
    filesize = os.stat(fileName).st_size 
    
    bytesDone = 0
    partitions = calcPartitions(fileName, blockSize)    
    i = 0
    allArgs = []
    for i in range(len(partitions)-1):
        args = (fileName,partitions[i], partitions[i+1]-partitions[i], i%threads)
        allArgs.append(args)
    
    numSections = len(allArgs)
    while len(allArgs) > 0:
        #run [threadcount] tasks
        p.map(sort_file_one_arg, allArgs[:min(threads, len(allArgs))]) 
        #remove tasks that are done
        allArgs = allArgs[min(threads,len(allArgs)):]
        
        prettyprint.printProgressBar(numSections-len(allArgs), numSections, prefix = 'Phase 1/4:', suffix = 'Complete', length = 50)
    
    # do a sort on all the mini-files
    charIndex = 0
    for char in choices:
        p.map(sort_completed_one_arg, ((i,char) for i in range(threads)))
        prettyprint.printProgressBar(charIndex+1, len(choices), prefix = 'Phase 2/4:', suffix = 'Complete', length = 50)
        charIndex += 1
    
    # now merge files
    charIndex = 0
    for char in choices:
        merge_sorted(char,threads)
        prettyprint.printProgressBar(charIndex+1, len(choices), prefix = 'Phase 3/4:', suffix = 'Complete', length = 50)
        charIndex += 1
        
    # final merge - one blob.
    charIndex = 0
    final = open('sorted.blob','w') #erase file
    for char in choices:
        merge_final(char)
        prettyprint.printProgressBar(charIndex+1, len(choices), prefix = 'Phase 4/4:', suffix = 'Complete', length = 50)
        charIndex += 1
    
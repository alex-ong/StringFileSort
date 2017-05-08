import string
from multiprocessing import Pool
import os
import time
import shutil

choices = string.ascii_uppercase

import prettyprint


def sort_file_one_arg(arg):
    fileName,start_byte,num_bytes,procID = arg
    sort_file(fileName,start_byte,num_bytes,procID)
    
#override this function with something if you want it to be processed
def process_string(input):
    #time.sleep(0.001)
    return input
    
def sort_file(fileName,start_byte,num_bytes,procID):
    with open(fileName, 'r') as f:
        f.seek(start_byte)
        tempStr = f.read(num_bytes)
        while not tempStr.endswith('\n'):
            char = f.read(1)
            if char == "":
                break
            tempStr += f.read(1)
            
    allEntries = tempStr.split('\n')
    
    files = {}
    for char in choices:
        f = open(getFile(char,procID), 'a+')
        files[char] = f
        
    for entry in allEntries:        
        if len(entry) > 0:
            newEntry = process_string(entry)
            files[newEntry[0]].write(newEntry + '\n')        
                
    for file in files.values():
        file.close()
        
def sort_completed_one_arg(args):
    procID, char = args
    sort_completed(procID, char)
    
def sort_completed(procID, char):         
    f = open(getFile(char,procID), 'r')
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
        f = open(getFile(char,i))
        files.append(f)
    
    final = open(getFile(char,'_final'),'w')
    
    currentCmp = []
    for file in files:
        currentCmp.append(file.readline().strip())
        
    while len(files) > 0:        
        minimum = min(currentCmp)
        index = currentCmp.index(minimum)
        final.write(minimum + '\n')
        
        newValue = files[index].readline()
        if not newValue:
            files[index].close()
            del files[index]
            del currentCmp[index]
        else:
            currentCmp[index] = newValue.strip()
    
    for fileName in (getFile(char, i) for i in range (numProcs)):
        os.remove(fileName)
        
def merge_final(char):
    final = open('sorted.blob','a+')
    merger = open(getFile(char,'_final'),'r')
    
    shutil.copyfileobj(merger, final)    
    
    final.close()
    merger.close()
    os.remove(getFile(char,'_final'))
    
if __name__ == '__main__':
    p = Pool(4)
    blockSize = 1024*1024*50
    fileName = 'rawblob.blob'
    filesize = os.stat(fileName).st_size 
    
    bytesDone = 0
    while bytesDone < filesize:
        allArgs = []
        for i in range (4):
            args = (fileName,bytesDone, blockSize, i)
            bytesDone += blockSize
            allArgs.append(args)
            
        p.map(sort_file_one_arg, allArgs) 
        prettyprint.printProgressBar(bytesDone, filesize, prefix = 'Phase 1:', suffix = 'Complete', length = 50)
    
    # do a sort on all the mini-files
    charIndex = 0
    for char in choices:
        p.map(sort_completed_one_arg, ((i,char) for i in range(4)))
        prettyprint.printProgressBar(charIndex, len(choices), prefix = 'Phase 2:', suffix = 'Complete', length = 50)
        charIndex += 1
    
    # now merge files
    charIndex = 0
    for char in choices[1:]:
        merge_sorted(char,4)
        prettyprint.printProgressBar(charIndex, len(choices), prefix = 'Phase 3:', suffix = 'Complete', length = 50)
        charIndex += 1
        
    # final merge - one blob.
    charIndex = 0
    for char in choices:
        merge_final(char)
        prettyprint.printProgressBar(charIndex, len(choices), prefix = 'Final Phase:', suffix = 'Complete', length = 50)
        charIndex += 1
    
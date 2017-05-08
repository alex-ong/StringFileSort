import string
from multiprocessing import Pool
import os
import time
choices = string.ascii_uppercase + string.ascii_lowercase

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
        f = open(char + str(procID) + '.txt', 'a+')
        files[char] = f
        
    for entry in allEntries:        
        if len(entry) > 0:
            newEntry = process_string(entry)
            files[newEntry[0]].write(newEntry + '\n')        
                
    for file in files.values():
        file.close()
        
        
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
    
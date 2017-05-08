import sys
import os
import time
from multiprocessing import Pool

def check_partial_one_arg(args):
    fileName, start_byte, numBytes = args
    check_partial(fileName, start_byte, numBytes)
    
def check_partial(fileName, start_byte, numBytes):
    bytesSoFar = 0
    with open(fileName, 'r') as f:
        f.seek(start_byte)
        
        if start_byte != 0:
            while True:
                byte = f.read()
                if byte == '\n' or not byte:
                    break            
        
        prevLine = None    
        for line in f:
            if prevLine is not None and prevLine.strip() > line.strip():
                print (prevLine.strip(),line.strip())
                
            bytesSoFar += len(line)
            prevLine = line
            if bytesSoFar > numBytes:
                break
            
if __name__ == '__main__':
    if len(sys.argv) < 3:
        print ('usage: checker.py fileName numThreads')
        sys.exit()
        
    fileName = sys.argv[1]
    numThreads = int(sys.argv[2])
    split_size = os.stat(fileName).st_size // numThreads
    
    allArgs = []
    for i in range(numThreads):
        args = (fileName, i * split_size, split_size)
        allArgs.append(args)
        
    p = Pool(numThreads)
    p.map(check_partial_one_arg, allArgs)     

    
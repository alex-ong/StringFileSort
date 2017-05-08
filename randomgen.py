import random
import string
import time
import sys

from multiprocessing import Pool
import itertools
import prettyprint

choices = string.ascii_uppercase

def gen_strings(numStrings, lenString=60):
    return ''.join((''.join(random.choice(choices) for _ in range(lenString)) + '\n') for x in range(numStrings))

if __name__ == '__main__':
    gigs = 0.25    
    stringLength = 60
    totalStrings = int(gigs*1024*1024*1024 / (stringLength + 1))
    numStrings = 0
        
    p = Pool(4)
    start = time.time()
    with open('rawblob.blob', 'w') as f:
        while (numStrings < totalStrings):
            s = ''.join([random.choice(choices) for _ in range(1024*1024)])
           
            results = p.map(gen_strings,[1024*30 for _ in range(4)])
            finalResult = ''.join(results)
            f.write(finalResult)
            numStrings += 1024*30*4
            prettyprint.printProgressBar(min(numStrings,totalStrings), totalStrings, prefix = 'Progress:', suffix = 'Complete', length = 50)
    print ("Complete, took " +str(time.time() - start) + "seconds")

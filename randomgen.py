import random
import string
import time
import sys

from multiprocessing import Pool
import itertools

# Print iterations progress
def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = 'x'):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end = '\r')
    # Print New Line on Complete
    if iteration == total: 
        print()

choices = string.ascii_uppercase + string.ascii_lowercase

def gen_strings(numStrings, lenString=60):
    return ''.join((''.join(random.choice(choices) for _ in range(lenString)) + '\n') for x in range(numStrings))

if __name__ == '__main__':
    gigs = 1    
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
            printProgressBar(numStrings, totalStrings, prefix = 'Progress:', suffix = 'Complete', length = 50)
    print ("Complete, took " +str(time.time() - start) + "seconds")

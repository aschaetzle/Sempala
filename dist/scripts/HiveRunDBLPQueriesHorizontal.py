'''
Created on Nov 1, 2013

@author: neua
'''


from sys import argv, exit
import os
from os import listdir
from os.path import isfile, join
import glob
import time
from HiveRunDBLPQuery import runQueryAllDatasets




timeout = 3

def getOpts(argv):
    opts = {}
    while argv:
        if argv[0][0] == '-':
            opts[argv[0]] = argv[1]
            argv = argv[2:]
        else:
            argv = argv[1:]
    return opts



def benchmarkDataset(folder, timeout):
    dir = glob.glob( os.path.join(folder, '*.sql'))
    for f in dir:
        print('Running file '+str(f))
        runQueryAllDatasets(f, timeout)
        print('Waiting '+str(timeout)+' seconds.')
        time.sleep(timeout)



def main():
    opts = getOpts(argv)
    folder = ""
    try: 
        timeout = int(opts['-t'])
    except KeyError:
        timeout = 3 
    try: 
        folder = opts['-f']
    except KeyError:
        print("no folder")
        exit()
    benchmarkDataset(folder, timeout)
   
if __name__ == '__main__':
    main()

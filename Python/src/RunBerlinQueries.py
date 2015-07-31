'''
Created on Nov 1, 2013

@author: neua
'''


from sys import argv, exit
import os
from os import listdir
from os.path import isfile, join
from ImpalaWrapper import ImpalaWrapper
import glob
import time






def getOpts(argv):
    opts = {}
    while argv:
        if argv[0][0] == '-':
            opts[argv[0]] = argv[1]
            argv = argv[2:]
        else:
            argv = argv[1:]
    return opts

def benchmarkAllDatasets(folder, timeout):
    datasets = ['dblp1M', 'dblp10M','dblp25M', 'dblp50M',  'dblp100M', 'dblp200M','dblp300M', 'dblp400M', 'dblp500M']
    for set in datasets:
        benchmarkDataset(folder, set, timeout)

def benchmarkDataset(folder, dataset, timeout):
    dir = glob.glob( os.path.join(folder, '*.sql'))
    logpath =  os.path.join(folder, "results"+dataset+".log")
    for f in dir:
        start = time.time()  
        impala = ImpalaWrapper(dataset+"_bigtable_hive")
        impala.queryFromFile( f )
        with open(logpath, "a") as log:
            log.write(f+"\t")
        runtime = str(time.time() - start)+" seconds"
        impala = ImpalaWrapper(dataset+"_bigtable_hive")
        impala.queryToStdOut('SELECT COUNT(*) FROM result;')
        with open(logpath, "a") as log:
            log.write(impala.getResults()[0].rstrip() + "\t")
            log.write(runtime + "\n")   
        print('Waiting '+str(timeout)+' seconds.')
        time.sleep(timeout)
   


def main():
    opts = getOpts(argv)
    folder = ""
    try:
        dataset = opts['-d']
    except KeyError:
        dataset = False
    try: 
        timeout = int(opts['-t'])
    except KeyError:
        timeout = 3 
    try: 
        folder = opts['-f']
    except KeyError:
        print("no folder")
        exit()
    if not dataset:
        benchmarkAllDatasets(folder, timeout)
    else: 
        benchmarkDataset(folder, dataset, timeout)
   
if __name__ == '__main__':
    main()

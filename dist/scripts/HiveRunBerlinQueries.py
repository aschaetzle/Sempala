'''
Created on Nov 1, 2013

@author: neua
'''


from sys import argv, exit
import os
from os import listdir
from os.path import isfile, join
from HiveWrapper import HiveWrapper
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
    datasets = ['berlin_500k', 'berlin_1000k','berlin_1500k', 'berlin_2000k',  'berlin_2500k', 'berlin_3000k']
    for set in datasets:
        benchmarkDataset(folder, set, timeout)

def benchmarkDataset(folder, dataset, timeout):
    dbname = dataset+"_bigtable_hive"
    directory = glob.glob( os.path.join(folder, '*.sql'))
    for f in directory:
        start = time.time()
        hive = HiveWrapper(dbname)
        hive.queryFromFile( f )
        with open(os.path.join(os.path.dirname(f), "results"+dataset+".log"), "a") as log:
                log.write(f+"\t")
                log.write(str(time.time() -start ) + "seconds \t")   
        hive = HiveWrapper(dbname)
        hive.queryToStdOut('SELECT COUNT(*) FROM hiveresult;')
        with open(os.path.join(os.path.dirname(f), "results"+dataset+".log"), "a") as log:
            log.write(hive.getResults()[0].rstrip() + "\n")
        print('Waiting '+str(timeout)+' seconds.')
        time.sleep(timeout)
    log.close()



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

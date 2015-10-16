'''
Created on Nov 1, 2013

@author: neua
'''


from sys import argv, exit
import os
from os import listdir
from os.path import isfile, join, dirname
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

def runQueryAllDatasets(file, timeout):
    datasets = [ 'lubm500','lubm1000','lubm1500', 'lubm2000', 'lubm2500', 'lubm3000']
    for set in datasets:
        print('Running dataset '+str(set))
        runQuery(file, set, timeout)

def runQuery(file, dataset, timeout):
    start = time.time()
    dbname = dataset+"_bigtable_hive"
    hive = HiveWrapper(dbname)
    hive.queryFromFile(file)
    with open(os.path.join(os.path.dirname(file), "results"+dataset+".log"), "a") as log:
        log.write(file+"\t")
        log.write(str(time.time() -start ) + "seconds \t")   
    
    hive = HiveWrapper(dbname)
    hive.queryToStdOut('SELECT COUNT(*) FROM hiveresult;')
    with open(os.path.join(os.path.dirname(file), "results"+dataset+".log"), "a") as log:
        log.write(hive.getResults()[0].rstrip() + "\n")
    print('Waiting '+str(timeout)+' seconds.')
    time.sleep(timeout)
    


def main():
    opts = getOpts(argv)
    try: 
        dataset = opts['-d']
    except KeyError:
        dataset = False
        print "No dataset given. Querying all data sets."
    try: 
        timeout = int(opts['-t'])
    except KeyError:
        timeout = 3 
    try: 
        file = opts['-f']
    except KeyError:
        print "no file given"
        exit()
    if not dataset:
        runQueryAllDatasets(file, timeout)
    else: 
        runQuery(file, dataset)
   
if __name__ == '__main__':
    main()

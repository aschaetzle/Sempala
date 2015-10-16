'''
Created on Nov 1, 2013

@author: neua
'''


from sys import argv, exit
import os
from os import listdir
from os.path import isfile, join, dirname
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

def runQueryAllDatasets(file, timeout):
    datasets = ['lubm5', 'lubm500','lubm1000','lubm1500', 'lubm2000', 'lubm2500', 'lubm3000']
    for set in datasets:
        print('Running dataset '+str(set))
        runQuery(file, set, timeout)

def runQuery(file, dataset, timeout):
    log = open( os.path.join(os.path.dirname(file), "results"+dataset+".log"), 'a')
    impala = ImpalaWrapper(dataset+"_bigtable")
    impala.queryFromFile(file)
    log.write(file+"\t")
    runtime = impala.getRuntime()
    impala = ImpalaWrapper(dataset+"_bigtable")
    impala.queryToStdOut('SELECT COUNT(*) FROM result;')
    log.write(impala.getResults()[0].rstrip() + "\t")
    log.write(runtime + "\n")   
    print('Waiting '+str(timeout)+' seconds.')
    time.sleep(timeout)
    log.close()
    


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

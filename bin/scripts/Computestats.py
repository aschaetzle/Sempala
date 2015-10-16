#!/usr/bin/env python

# author: Antony Neu
# Python version: 2.5

import os
from string import Template
import subprocess
from sys import argv
import sys

from ImpalaWrapper import ImpalaWrapper


# Parse command line arguments
def getOpts(argv):
    opts = {}
    while argv:
        if argv[0][0] == '-':
            opts[argv[0]] = argv[1]
            argv = argv[2:]
        else:
            argv = argv[1:]
    return opts



def computeStats(dataset):
    impala = ImpalaWrapper(dataset)
    impala.computeStats()
        
    


def main():
    opts = getOpts(argv)
    try:
        dataset = opts['-d']
    except KeyError:
        print("no dataset specified")
        sys.exit(10)
    
    # load single dataset
    computeStats(dataset)
        

if __name__ == "__main__":
    main()

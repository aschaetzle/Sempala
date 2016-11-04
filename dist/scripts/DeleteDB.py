#!/usr/bin/env python

# author: Antony Neu
# Python version: 2.5

from sys import argv
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


def main():
    opts = getOpts(argv)
    dataset = opts['-d']
    
    if not dataset:
        SystemExit()
    
    
    impala = ImpalaWrapper(dataset)
    impala.clearDatabase()


if __name__ == "__main__":
    main()

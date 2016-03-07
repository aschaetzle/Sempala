#!/usr/bin/env python

# author: Antony Neu
# Python version: 2.5


# Splits Berlin queries into separate files and generate template queries.


import os
from string import Template
import subprocess
from sys import argv


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


    
    
def splitFile(file, folder):
    if not os.path.exists(folder):
        os.makedirs(folder)    
    
    
    input = open(file, "r")
    initial = True
    text = ""
    list = []
    newFile = True
    for line in input: 
        if (initial and not "PREFIX" in line):
            print("Deleting: "+ line)
        elif  (not initial and "ms" in line and "total:" in line):
            break
        else: 
            if newFile and "PREFIX" in line:
                newFile = False
                if not initial:
                    list.append(text)
                text = line
            else: 
                text = text + line
                if (not newFile and not "PREFIX" in line):
                    newFile = True
            initial = False
    
    #last text 
    list.append(text)        
    input.close()        
    
    # Generate separate files
    for i, val in enumerate(list):
        outfile = open (os.path.join(folder, "Q"+str(i+1)+".sparql"), "w")
        outfile.write(val)
        outfile.close()


def main():
    opts = getOpts(argv)
    try:
        file = opts['-f']
        folder = opts['-o']
        splitFile(file, folder)
    except KeyError:
        print("File or output folder missing.")
        
    
    
        

if __name__ == "__main__":
    main()

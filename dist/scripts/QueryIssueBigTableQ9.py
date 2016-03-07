'''
Created on Nov 1, 2013

@author: neua
'''

import os
from sys import argv

from ImpalaWrapper import ImpalaWrapper


def getOpts(argv):
    opts = {}
    while argv:
        if argv[0][0] == '-':
            opts[argv[0]] = argv[1]
            argv = argv[2:]
        else:
            argv = argv[1:]
    return opts

def issueQueryBigTable(dataset, tablename, logfilename):
    impala = ImpalaWrapper(dataset)
    d = os.path.dirname(logfilename)
    if not os.path.exists(d):
        os.makedirs(d)
    log = open(logfilename, "w")
    log.write('QUERY\t#RESULTS\tRUNTIME\tDATASET\n')


    queries = []
    names = ['Q9', 'Q9ohneOrder']

   
    # angelehnt an Q2 mit ORDER BY 
    query9 = r'''select distinct id, rdf_type, dc_creator, bench_booktitle, dc_title, dcterms_partOf,  rdfs_seeAlso, 
    swrc_pages, foaf_homepage, dcterms_issued, bench_abstract from ''' + tablename + r''' 
    WHERE rdf_type = "bench:Inproceedings" and  dc_creator is not null and bench_booktitle is not null and  dc_title is not null and  dcterms_partOf is not null 
    and rdfs_seeAlso is not null and swrc_pages is not null and foaf_homepage is not null and dcterms_issued is not null  ;''' 
    queries.append(query9)



    print "Querying database..."

    for index, query in enumerate(queries):
        impala.query(query)
        print names[index] + " finished."
        log.write(names[index] + "\t")
        log.write(impala.getNumResults() + "\t")
        log.write(impala.getRuntime() + "\t")
        log.write(dataset)
        log.write("\n")
        log.flush()

    log.close()


def main():
    opts = getOpts(argv)
    dataset = opts['-d']
    
    tablename = opts['-tn']
    if not tablename:
        print("ERROR. No tablename given.")
        SystemExit()


    logfile = opts['-l']    
    
    issueQueryBigTable(dataset, tablename, logfile)


if __name__ == '__main__':
    main()

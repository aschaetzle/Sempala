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

def issueQueryBigTable(dataset, logfilename):
    impala = ImpalaWrapper(dataset)
    d = os.path.dirname(logfilename)
    if not os.path.exists(d):
        os.makedirs(d)
    log = open(logfilename, "w")
    log.write('QUERY\t#RESULTS\tRUNTIME\tDATASET\n')


    queries = []
    names = ['Q1','Q2', 'Q3']

   
    query9 = r'''select distinct id, rdf_type, dc_creator, dc_title, 
    swrc_pages, dcterms_issued from bigtable_parquet_snappy 
    WHERE rdf_type = "bench:Article" and dc_creator is not null and dc_title is not null and swrc_pages is not null ;''' 
    queries.append(query9)
    
    
    query9 = r'''select distinct id, foaf_name from bigtable_parquet_snappy 
    WHERE rdf_type = "foaf:Person" and foaf_name is not null ;''' 
    queries.append(query9)
    
    query9 = r'''SELECT * FROM (select distinct id, rdf_type, dc_creator, dc_title, 
    swrc_pages, dcterms_issued from bigtable_parquet_snappy 
    WHERE rdf_type = "bench:Article" and dc_creator is not null and dc_title is not null and swrc_pages is not null and dcterms_issued is not null) T1 join (select distinct id, foaf_name from bigtable_parquet_snappy 
    WHERE rdf_type = "foaf:Person" and foaf_name is not null ) T2 on (T1.dc_creator = T2.ID)  ;''' 
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
    

    logfile = opts['-l']    
    
    issueQueryBigTable(dataset, logfile)


if __name__ == '__main__':
    main()

'''
Created on Nov 1, 2013

@author: neua
'''

import os.path
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

def issueQuery(dataset, logfilename, suffix = ""):
    d = os.path.dirname(logfilename)
    if not os.path.exists(d):
        os.makedirs(d)
    logfile = logfilename 
    log = open(logfile, "w")
    log.write('QUERY\t#RESULTS\tRUNTIME\tDATASET\n')


   
    # query created index table
    query1 = 'SELECT * FROM vertical_index_parquet;'
    impala = ImpalaWrapper(dataset)
    impala.queryToStdOut(query1)
    tables = impala.getResults()
    index = dict()
    for table in tables:
        splitted = table.split()
        index[splitted[1]] = splitted[0]+suffix
    

    queries = []



    names = ['Q6.1', 'Q6.2', 'Q6.3', 'Q6.4', 'Q7.1', 'Q7.2', 'Q7.3', 'Q7.4,', 'Q6.1[partitioned]', 'Q6.2[partitioned]', 'Q6.3[partitioned]', 'Q6.4[partitioned]', 'Q7.1[partitioned]', 'Q7.2[partitioned]', 'Q7.3[partitioned]', 'Q7.4[partitioned]']

    # Deep Chained joins no filter
    query6 = r'select * from ' + index['dcterms:references'] + r' t1 join ' + index['rdf:_1'] + r' t2 join ' + index['dc:creator'] + r' t3 join ' + index['foaf:name'] + r''' t4 
    on(t1.object = t2.subject and t2.object = t3.subject and t3.object = t4.subject);'''
    queries.append(query6) 

    query6 = r'select * from ' + index['dc:creator'] + r' t3 join '+index['foaf:name'] + ' t4 join '+ index['rdf:_1'] + r' t2 join ' + index['dcterms:references'] + r' t1 '  + r''' 
    on(t1.object = t2.subject and t2.object = t3.subject and t3.object = t4.subject) ;'''
    queries.append(query6) 
    
    query6 = r'select * from ' + index['rdf:_1'] + r' t2 join ' + index['dcterms:references'] + r' t1 join ' + index['dc:creator'] + r' t3 join ' + index['foaf:name'] + r''' t4 
    on(t1.object = t2.subject and t2.object = t3.subject and t3.object = t4.subject) ;'''
    queries.append(query6) 
    
    query6 = r'select * from ' + index['foaf:name'] + r' t4 join ' + index['dc:creator'] + r' t3 join ' + index['rdf:_1'] + r' t2 join ' + index['dcterms:references'] + r''' t1 
    on(t1.object = t2.subject and t2.object = t3.subject and t3.object = t4.subject) ;'''
    queries.append(query6) 


    # Deep Chained joins with filter
    query7 = r'SELECT * FROM (select * from ' + index['dcterms:references'] + r' WHERE subject = "<http://localhost/publications/inprocs/Proceeding567/2001/Inproceeding33402>")t1 join ' + index['rdf:_1'] + r' t2 join ' + index['dc:creator'] + r' t3 join ' + index['foaf:name'] + r''' t4 
    on(t1.subject = "<http://localhost/publications/inprocs/Proceeding567/2001/Inproceeding33402>" and t1.object = t2.subject and t2.object = t3.subject and t3.object = t4.subject) ;'''
    queries.append(query7)
    
    
    query7 = r'select * from ' + index['dc:creator'] + r' t3 join '+index['foaf:name'] + ' t4 join '+ index['rdf:_1'] + r' t2 join ' + index['dcterms:references'] + r' t1 '  + r''' 
    on(t1.subject = "<http://localhost/publications/inprocs/Proceeding567/2001/Inproceeding33402>" and t1.object = t2.subject and t2.object = t3.subject and t3.object = t4.subject) ;'''
    queries.append(query7) 
    
    query7 = r'select * from ' + index['rdf:_1'] + r' t2 join ' + index['dcterms:references'] + r' t1 join ' + index['dc:creator'] + r' t3 join ' + index['foaf:name'] + r''' t4 
    on(t1.subject = "<http://localhost/publications/inprocs/Proceeding567/2001/Inproceeding33402>" and t1.object = t2.subject and t2.object = t3.subject and t3.object = t4.subject) ;'''
    queries.append(query7) 
    
    query7 = r'select * from ' + index['foaf:name'] + r' t4 join ' + index['dc:creator'] + r' t3 join ' + index['rdf:_1'] + r' t2 join ' + index['dcterms:references'] + r''' t1 
    on(t1.subject = "<http://localhost/publications/inprocs/Proceeding567/2001/Inproceeding33402>" and t1.object = t2.subject and t2.object = t3.subject and t3.object = t4.subject) ;'''
    queries.append(query7) 
    
    
    
    # Deep Chained joins no filter
    query6 = r'select STRAIGHT_JOIN * from ' + index['dcterms:references'] + r' t1 join [shuffle] ' + index['rdf:_1'] + r' t2 join [shuffle] ' + index['dc:creator'] + r' t3 join [shuffle] ' + index['foaf:name'] + r''' t4 
    on(t1.object = t2.subject and t2.object = t3.subject and t3.object = t4.subject);'''
    queries.append(query6) 

    query6 = r'select STRAIGHT_JOIN * from ' + index['dc:creator'] + r' t3 join [shuffle] '+index['foaf:name'] + ' t4 join [shuffle] '+ index['rdf:_1'] + r' t2 join [shuffle] ' + index['dcterms:references'] + r' t1 '  + r''' 
    on(t1.object = t2.subject and t2.object = t3.subject and t3.object = t4.subject) ;'''
    queries.append(query6) 
    
    query6 = r'select STRAIGHT_JOIN * from ' + index['rdf:_1'] + r' t2 join [shuffle] ' + index['dcterms:references'] + r' t1 join [shuffle] ' + index['dc:creator'] + r' t3 join [shuffle] ' + index['foaf:name'] + r''' t4 
    on(t1.object = t2.subject and t2.object = t3.subject and t3.object = t4.subject) ;'''
    queries.append(query6) 
    
    query6 = r'select STRAIGHT_JOIN * from ' + index['foaf:name'] + r' t4 join [shuffle] ' + index['dc:creator'] + r' t3 join [shuffle] ' + index['rdf:_1'] + r' t2 join [shuffle] ' + index['dcterms:references'] + r''' t1 
    on(t1.object = t2.subject and t2.object = t3.subject and t3.object = t4.subject) ;'''
    queries.append(query6) 
    
        # Deep Chained joins with filter
    query7 = r'SELECT STRAIGHT_JOIN * FROM (select * from ' + index['dcterms:references'] + r' WHERE subject = "<http://localhost/publications/inprocs/Proceeding567/2001/Inproceeding33402>")t1 join  [shuffle] ' + index['rdf:_1'] + r' t2 join  [shuffle]  ' + index['dc:creator'] + r' t3 join  [shuffle] ' + index['foaf:name'] + r''' t4 
    on(t1.subject = "<http://localhost/publications/inprocs/Proceeding567/2001/Inproceeding33402>" and t1.object = t2.subject and t2.object = t3.subject and t3.object = t4.subject) ;'''
    queries.append(query7)
    
    
    query7 = r'select STRAIGHT_JOIN * from ' + index['dc:creator'] + r' t3 join  [shuffle] '+index['foaf:name'] + ' t4 join  [shuffle] '+ index['rdf:_1'] + r' t2 join  [shuffle] ' + index['dcterms:references'] + r' t1 '  + r''' 
    on(t1.subject = "<http://localhost/publications/inprocs/Proceeding567/2001/Inproceeding33402>" and t1.object = t2.subject and t2.object = t3.subject and t3.object = t4.subject) ;'''
    queries.append(query7) 
    
    query7 = r'select STRAIGHT_JOIN * from ' + index['rdf:_1'] + r' t2 join  [shuffle]  ' + index['dcterms:references'] + r' t1 join  [shuffle] ' + index['dc:creator'] + r' t3 join [shuffle]  ' + index['foaf:name'] + r''' t4 
    on(t1.subject = "<http://localhost/publications/inprocs/Proceeding567/2001/Inproceeding33402>" and t1.object = t2.subject and t2.object = t3.subject and t3.object = t4.subject) ;'''
    queries.append(query7) 
    
    query7 = r'select STRAIGHT_JOIN * from ' + index['foaf:name'] + r' t4 join  [shuffle]  ' + index['dc:creator'] + r' t3 join  [shuffle] ' + index['rdf:_1'] + r' t2 join  [shuffle] ' + index['dcterms:references'] + r''' t1 
    on(t1.subject = "<http://localhost/publications/inprocs/Proceeding567/2001/Inproceeding33402>" and t1.object = t2.subject and t2.object = t3.subject and t3.object = t4.subject) ;'''
    queries.append(query7) 


    


    # angelehnt an SP2Bench Q10 
 
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

    tablesuffix = opts['-ts']
    
    if tablesuffix != "":
        print("Querying tables with suffix "+tablesuffix)

    logfilename = opts['-l']   
    
    issueQuery(dataset, logfilename, tablesuffix)

if __name__ == '__main__':
    main()

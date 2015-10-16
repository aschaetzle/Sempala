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


    # single pattern, Objekt gebunden, sehr selektiv
    query1 = r'select * from ' + index['dc:title'] +  r' where object = "\"Journal 1 \(1940\)\"\^\^xsd:string";'
    queries.append(query1)    
    

    # single pattern, Subjekt gebunden, sehr selektiv
    query2 = r'select * from ' + index['dc:title'] + r' where subject = "<http://localhost/publications/journals/Journal1/1940>";'
    queries.append(query2)     

    # single pattern, subjekt ungebunden, unselektiv
    # inproceedings noch mehr
    query3 = r'select * from ' + index['rdf:type'] + r' where object = "bench:Article";'
    queries.append(query3)  

    # Fast Q1 als join 
    query4 = r'select * from ' + index['rdf:type'] + r' t1 join ' + index['dc:title'] + r''' t2 on(t1.subject = t2.subject 
                and  t2.object = "\"Journal 1 \(1940\)\"\^\^xsd:string") ;'''
    queries.append(query4)  


    # Fast Q1 als join mit gefiltertem subquery
    query5 = r'select * from ' + index['rdf:type'] + r' t1 join (Select * from ' + index['dc:title'] + r' where object = "\"Journal 1 \(1940\)\"\^\^xsd:string") t2 on(t1.subject = t2.subject ) ;'
    queries.append(query5)  

    # Deep Chained joins no filter
    query6 = r'select * from ' + index['dcterms:references'] + r' t1 join ' + index['rdf:_1'] + r' t2 join ' + index['dc:creator'] + r' t3 join ' + index['foaf:name'] + r''' t4 
    on(t1.object = t2.subject and t2.object = t3.subject and t3.object = t4.subject) ;'''
    queries.append(query6) 

    # Deep Chained joins with filter
    query7 = r'select * from ' + index['dcterms:references'] + r' t1 join ' + index['rdf:_1'] + r' t2 join ' + index['dc:creator'] + r' t3 join ' + index['foaf:name'] + r''' t4 
          on(t1.subject = "<http://localhost/publications/inprocs/Proceeding567/2001/Inproceeding33402>" and t1.object = t2.subject and t2.object = t3.subject and t3.object = t4.subject) ;'''
    queries.append(query7) 
    
    
    # Wide star joins

    query8 = r'select * from ' + index['rdf:type'] + r' t1 join ' + index['swrc:pages'] + r' t2 join ' + index['dc:creator'] + r' t3 join ' + index['dc:title'] + r''' t4 
       join ''' + index['swrc:month'] + r''' t5 join ''' + index['swrc:note'] + r''' t6
        on(t1.subject = t2.subject and t2.subject = t3.subject and t3.subject = t4.subject and t4.subject = t5.subject and t5.subject = t6.subject) ;'''
    queries.append(query8)

    # angelehnt an Q2 mit ORDER BY 
    query9 = (r'select * from ' + index['rdf:type'] + r' t1 join ' + 
              index['dc:creator'] + r' t2 join ' + index['bench:booktitle'] + r' t3 join ' + index['dc:title'] + r''' t4 
       join ''' + index['dcterms:partOf'] + r''' t5 join ''' + index['rdfs:seeAlso'] + r' t6 join ' + index['swrc:pages'] + r' t7 join ' + index['foaf:homepage'] + ' t8 ' + r'''
       join ''' + index['dcterms:issued'] + r''' t9 
        on(t1.subject = t2.subject and t1.object = "bench:Inproceedings"
        and t2.subject = t3.subject and t3.subject = t4.subject and t4.subject = t5.subject and t5.subject = t6.subject and t6.subject = t7.subject
        and t7.subject = t8.subject and t8.subject = t9.subject) left join ''' + index['bench:abstract'] + r''' t10 on(t9.subject = t10.subject)  
        ORDER BY t9.object LIMIT 100000000;''')
    queries.append(query9)


    # angelehnt an SP2Bench Q10 
    tables = index.items()
    query10 = r'''select * from ''' + index[tables[0][0]] + r''' where object = "<http://localhost/persons/Paul_Erdoes>" '''
    for table in tables[1:]:
        query10 = query10 +r'''  UNION ALL select * from ''' + index[table[0]] + r''' where object = "<http://localhost/persons/Paul_Erdoes>" '''
    query10 = query10 + ';';
    queries.append(query10) 

    print "Querying database..."

    for index, query in enumerate(queries):
        impala.query(query)
        print "Q" + str(index + 1) + " finished."
        log.write("Q" + str(index + 1) + "\t")
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

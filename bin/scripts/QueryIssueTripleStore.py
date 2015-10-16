'''
Created on Nov 1, 2013

@author: neua
'''

from ImpalaWrapper import ImpalaWrapper
from sys import argv
import os

def getOpts(argv):
    opts = {}
    while argv:
        if argv[0][0] == '-':
            opts[argv[0]] = argv[1]
            argv = argv[2:]
        else:
            argv = argv[1:]
    return opts

def issueQueryTripleStore(dataset, tablename, logfilename):
    impala = ImpalaWrapper(dataset)
    d = os.path.dirname(logfilename)
    if not os.path.exists(d):
        os.makedirs(d)
    log = open(logfilename, "w")
    log.write('QUERY\t#RESULTS\tRUNTIME\tDATASET\n')


    queries = []


    # single pattern, Objekt gebunden, sehr selektiv
    query1 = r'select * from ' + tablename + r' where predicate="dc:title" and object = "\"Journal 1 \(1940\)\"\^\^xsd:string";'
    queries.append(query1)    
    

    # single pattern, Subjekt gebunden, sehr selektiv
    query2 = r'select * from ' + tablename + r' where predicate="dc:title" and subject = "<http://localhost/publications/journals/Journal1/1940>";'
    queries.append(query2)     

    # single pattern, subjekt ungebunden, unselektiv
    # inproceedings noch mehr
    query3 = r'select * from ' + tablename + r' where predicate = "rdf:type" and object = "bench:Article";'
    queries.append(query3)  

    # Fast Q1 als join 
    query4 = r'select * from ' + tablename + r''' t1 join '''+tablename+r''' t2 on(t1.subject = t2.subject 
                and t1.predicate ="rdf:type" and t2.predicate="dc:title" and  t2.object = "\"Journal 1 \(1940\)\"\^\^xsd:string") ;'''
    queries.append(query4)  


    # Fast Q1 als join mit gefiltertem subquery
    query5 = r'select * from ' + tablename + r' t1 join (Select * from ' + tablename + r' where predicate = "dc:title" and object = "\"Journal 1 \(1940\)\"\^\^xsd:string") as t2 on(t1.subject = t2.subject ) where t1.predicate ="rdf:type" ;'
    queries.append(query5)  
    
    # Deep Chained joins no filter
    query6 = r'select * from ' + tablename + r' t1 join ' + tablename + r' t2 join ' + tablename + r' t3 join ' + tablename + r''' t4 
    on(t1.predicate = "dcterms:references" and t2.predicate = "rdf:_1" and t3.predicate = "dc:creator" and t4.predicate = "foaf:name" and t1.object = t2.subject and t2.object = t3.subject and t3.object = t4.subject) ;'''
    queries.append(query6) 

    # Deep Chained joins with filter
    query7 = r'select * from ' + tablename + r' t1 join ' + tablename + r' t2 join ' + tablename + r' t3 join ' + tablename + r''' t4 
          on(t1.predicate = "dcterms:references" and t2.predicate="rdf:_1" and t3.predicate ="dc:creator" and t4.predicate="foaf:name" and t1.subject = "<http://localhost/publications/inprocs/Proceeding567/2001/Inproceeding33402>" and t1.object = t2.subject and t2.object = t3.subject and t3.object = t4.subject) ;'''
    queries.append(query7) 
    
    
    # Wide star joins
    query8 = r'select * from ' + tablename + r' t1 join ' + tablename + r' t2 join ' + tablename + r' t3 join ' + tablename + r''' t4 
       join ''' + tablename + r''' t5 join ''' + tablename + r''' t6
        on(t1.predicate ="rdf:type" and t2.predicate="swrc:pages" and t3.predicate = "dc:creator" and t4.predicate ="dc:title" and t5.predicate = "swrc:month" and t6.predicate ="swrc:note"
        and t1.subject = t2.subject and t2.subject = t3.subject and t3.subject = t4.subject and t4.subject = t5.subject and t5.subject = t6.subject) ;'''
    queries.append(query8) 
    
    
    # angelehnt an Q2 mit ORDER BY 
    query9 = (r'select * from ' + tablename + r' t1 join ' + 
              tablename + r' t2 join ' + tablename + r' t3 join ' + tablename + r''' t4 
       join ''' + tablename + r''' t5 join ''' + tablename + r' t6 join ' + tablename + r' t7 join ' + tablename + ' t8 ' + r'''
       join ''' + tablename + r''' t9 
        on(t1.predicate = "rdf:type" and t2.predicate = "dc:creator" and t3.predicate ="bench:booktitle" and t4.predicate ="dc:title"
        and t5.predicate = "dcterms:partOf" and t6.predicate ="rdfs:seeAlso" and t7.predicate = "swrc:pages" and t8.predicate="foaf:homepage"
        and t9.predicate="dcterms:issued"
        and t1.subject = t2.subject and t1.object = "bench:Inproceedings"
        and t2.subject = t3.subject and t3.subject = t4.subject and t4.subject = t5.subject and t5.subject = t6.subject and t6.subject = t7.subject
        and t7.subject = t8.subject and t8.subject = t9.subject) left join ''' + tablename + r''' t10 on(t10.predicate ="bench:abstract" and t9.subject = t10.subject)  
        ORDER BY t9.object LIMIT 100000000;''')
    queries.append(query9)


    # angelehnt an SP2Bench Q10
    query10 = r'''select * from ''' + tablename + r''' where object = "<http://localhost/persons/Paul_Erdoes>"; '''
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
    
    tablename = opts['-tn']
    if not tablename:
        print("ERROR. No tablename given.")
        SystemExit()


    logfile = opts['-l']    
    
    issueQueryTripleStore(dataset, tablename, logfile)


if __name__ == '__main__':
    main()

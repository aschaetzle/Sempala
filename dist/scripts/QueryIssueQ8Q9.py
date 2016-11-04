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

def issueQuery(dataset, logfilename, suffix=""):
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
        index[splitted[1]] = splitted[0] + suffix
    

    queries = []



    names = [#'Q8.1', 'Q8.2','Q8.3','Q8.4','Q8.5','Q8.6', 'Q8.1 straight', 'Q8.2 straight','Q8.3 straight','Q8.4 straight','Q8.5straight','Q8.6straight', '
             'Q9.1', 'Q9.1ohneORDER', 'Q9.1ohneOrderundOpt', 'Q9.2', 'Q9.3', 'Q9.4', 'Q9.5', 'Q9.6'  ]

    # Deep Chained joins no filter
    query8 = r'select * from ' + index['rdf:type'] + r' t1 join ' + index['swrc:pages'] + r' t2 join ' + index['dc:creator'] + r' t3 join ' + index['dc:title'] + r''' t4 
        join ''' + index['swrc:month'] + r''' t5 join ''' + index['swrc:note'] + r''' t6
         on(t1.subject = t2.subject and t2.subject = t3.subject and t3.subject = t4.subject and t4.subject = t5.subject and t5.subject = t6.subject) ;'''
#    queries.append(query8)
    
    query8 = r'select * from ' + index['dc:creator'] + r' t1 join ' + index['rdf:type'] + r' t2 join ' + index['swrc:pages'] + r' t3 join ' + index['dc:title'] + r''' t4 
        join ''' + index['swrc:month'] + r''' t5 join ''' + index['swrc:note'] + r''' t6
         on(t1.subject = t2.subject and t2.subject = t3.subject and t3.subject = t4.subject and t4.subject = t5.subject and t5.subject = t6.subject) ;'''
#    queries.append(query8)
    
    query8 = r'select * from ' + index['swrc:pages'] + r' t1 join ' + index['rdf:type']  + r' t2 join ' + index['dc:creator'] + r' t3 join ' + index['dc:title'] + r''' t4 
        join ''' + index['swrc:month'] + r''' t5 join ''' + index['swrc:note'] + r''' t6
         on(t1.subject = t2.subject and t2.subject = t3.subject and t3.subject = t4.subject and t4.subject = t5.subject and t5.subject = t6.subject) ;'''
#    queries.append(query8)
    
    query8 = r'select * from ' + index['dc:title']  + r' t1 join ' + index['rdf:type']  + r' t2 join ' +index['swrc:pages']  + r' t3 join ' + index['dc:creator']  + r''' t4 
        join ''' + index['swrc:month'] + r''' t5 join ''' + index['swrc:note'] + r''' t6
         on(t1.subject = t2.subject and t2.subject = t3.subject and t3.subject = t4.subject and t4.subject = t5.subject and t5.subject = t6.subject) ;'''
#    queries.append(query8)
    
    query8 = r'select * from ' + index['dc:title']  + r' t1 join ' +  index['swrc:note']   + r' t2 join ' +index['swrc:pages']  + r' t3 join ' + index['dc:creator']  + r''' t4 
        join ''' + index['swrc:month'] + r''' t5 join ''' + index['rdf:type'] + r''' t6
         on(t1.subject = t2.subject and t2.subject = t3.subject and t3.subject = t4.subject and t4.subject = t5.subject and t5.subject = t6.subject) ;'''
#    queries.append(query8)
    
    query8 = r'select * from ' +   index['swrc:month'] + r' t1 join ' +  index['swrc:note'] + r' t2 join ' +index['swrc:pages']  + r' t3 join ' + index['dc:creator']  + r''' t4 
        join ''' +index['dc:title']   + r''' t5 join ''' + index['rdf:type']  + r''' t6
         on(t1.subject = t2.subject and t2.subject = t3.subject and t3.subject = t4.subject and t4.subject = t5.subject and t5.subject = t6.subject) ;'''
#    queries.append(query8)


    # Deep Chained joins no filter
    query8 = r'select STRAIGHT_JOIN * from ' + index['rdf:type'] + r' t1 join ' + index['swrc:pages'] + r' t2 join ' + index['dc:creator'] + r' t3 join ' + index['dc:title'] + r''' t4 
        join ''' + index['swrc:month'] + r''' t5 join ''' + index['swrc:note'] + r''' t6
         on(t1.subject = t2.subject and t2.subject = t3.subject and t3.subject = t4.subject and t4.subject = t5.subject and t5.subject = t6.subject) ;'''
#    queries.append(query8)
    
    query8 = r'select STRAIGHT_JOIN * from ' + index['dc:creator'] + r' t1 join ' + index['rdf:type'] + r' t2 join ' + index['swrc:pages'] + r' t3 join ' + index['dc:title'] + r''' t4 
        join ''' + index['swrc:month'] + r''' t5 join ''' + index['swrc:note'] + r''' t6
         on(t1.subject = t2.subject and t2.subject = t3.subject and t3.subject = t4.subject and t4.subject = t5.subject and t5.subject = t6.subject) ;'''
#    queries.append(query8)
    
    query8 = r'select STRAIGHT_JOIN * from ' + index['swrc:pages'] + r' t1 join ' + index['rdf:type']  + r' t2 join ' + index['dc:creator'] + r' t3 join ' + index['dc:title'] + r''' t4 
        join ''' + index['swrc:month'] + r''' t5 join ''' + index['swrc:note'] + r''' t6
         on(t1.subject = t2.subject and t2.subject = t3.subject and t3.subject = t4.subject and t4.subject = t5.subject and t5.subject = t6.subject) ;'''
#    queries.append(query8)
    
    query8 = r'select STRAIGHT_JOIN * from ' + index['dc:title']  + r' t1 join ' + index['rdf:type']  + r' t2 join ' +index['swrc:pages']  + r' t3 join ' + index['dc:creator']  + r''' t4 
        join ''' + index['swrc:month'] + r''' t5 join ''' + index['swrc:note'] + r''' t6
         on(t1.subject = t2.subject and t2.subject = t3.subject and t3.subject = t4.subject and t4.subject = t5.subject and t5.subject = t6.subject) ;'''
#    queries.append(query8)
    
    query8 = r'select STRAIGHT_JOIN * from ' + index['dc:title']  + r' t1 join ' +  index['swrc:note']   + r' t2 join ' +index['swrc:pages']  + r' t3 join ' + index['dc:creator']  + r''' t4 
        join ''' + index['swrc:month'] + r''' t5 join ''' + index['rdf:type'] + r''' t6
         on(t1.subject = t2.subject and t2.subject = t3.subject and t3.subject = t4.subject and t4.subject = t5.subject and t5.subject = t6.subject) ;'''
#    queries.append(query8)
    
    query8 = r'select STRAIGHT_JOIN * from ' +   index['swrc:month'] + r' t1 join ' +  index['swrc:note'] + r' t2 join ' +index['swrc:pages']  + r' t3 join ' + index['dc:creator']  + r''' t4 
        join ''' +index['dc:title']   + r''' t5 join ''' + index['rdf:type']  + r''' t6
         on(t1.subject = t2.subject and t2.subject = t3.subject and t3.subject = t4.subject and t4.subject = t5.subject and t5.subject = t6.subject) ;'''
#    queries.append(query8)
    
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

    
    
    # angelehnt an Q2 ohne ORDER BY 
    query9 = (r'select * from ' + index['rdf:type'] + r' t1 join ' + 
              index['dc:creator'] + r' t2 join ' + index['bench:booktitle'] + r' t3 join ' + index['dc:title'] + r''' t4 
       join ''' + index['dcterms:partOf'] + r''' t5 join ''' + index['rdfs:seeAlso'] + r' t6 join ' + index['swrc:pages'] + r' t7 join ' + index['foaf:homepage'] + ' t8 ' + r'''
       join ''' + index['dcterms:issued'] + r''' t9 
        on(t1.subject = t2.subject and t1.object = "bench:Inproceedings"
        and t2.subject = t3.subject and t3.subject = t4.subject and t4.subject = t5.subject and t5.subject = t6.subject and t6.subject = t7.subject
        and t7.subject = t8.subject and t8.subject = t9.subject) left join ''' + index['bench:abstract'] + r''' t10 on(t9.subject = t10.subject) ;''')
    queries.append(query9)
    
        
    # angelehnt an Q2 ohne ORDER BY und ohne optional 
    query9 = (r'select * from ' + index['rdf:type'] + r' t1 join ' + 
              index['dc:creator'] + r' t2 join ' + index['bench:booktitle'] + r' t3 join ' + index['dc:title'] + r''' t4 
       join ''' + index['dcterms:partOf'] + r''' t5 join ''' + index['rdfs:seeAlso'] + r' t6 join ' + index['swrc:pages'] + r' t7 join ' + index['foaf:homepage'] + ' t8 ' + r'''
       join ''' + index['dcterms:issued'] + r''' t9 
        on(t1.subject = t2.subject and t1.object = "bench:Inproceedings"
        and t2.subject = t3.subject and t3.subject = t4.subject and t4.subject = t5.subject and t5.subject = t6.subject and t6.subject = t7.subject
        and t7.subject = t8.subject and t8.subject = t9.subject) ;''')
    queries.append(query9)

    
    
    #Q9.2
    query9 = (r'select STRAIGHT_JOIN * from ' + index['dc:title']  + r' t1 join ' + 
              index['dc:creator'] + r' t2 join ' + index['bench:booktitle'] + r' t3 join ' +  index['rdf:type'] + r''' t4 
       join ''' + index['dcterms:partOf'] + r''' t5 join ''' + index['rdfs:seeAlso'] + r' t6 join ' + index['swrc:pages'] + r' t7 join ' + index['foaf:homepage'] + ' t8 ' + r'''
       join ''' + index['dcterms:issued'] + r''' t9 
        on(t1.subject = t2.subject and t1.object = "bench:Inproceedings"
        and t2.subject = t3.subject and t3.subject = t4.subject and t4.subject = t5.subject and t5.subject = t6.subject and t6.subject = t7.subject
        and t7.subject = t8.subject and t8.subject = t9.subject) left join ''' + index['bench:abstract'] + r''' t10 on(t9.subject = t10.subject)  
        ORDER BY t9.object LIMIT 100000000;''')
    queries.append(query9)


    #Q9.3
    query9 = (r'select STRAIGHT_JOIN  * from ' + index['dcterms:issued']  + r' t1 join ' + 
              index['dc:creator'] + r' t2 join ' + index['bench:booktitle'] + r' t3 join ' +  index['rdf:type'] + r''' t4 
       join ''' + index['dcterms:partOf'] + r''' t5 join ''' + index['rdfs:seeAlso'] + r' t6 join ' + index['swrc:pages'] + r' t7 join ' + index['foaf:homepage'] + ' t8 ' + r'''
       join ''' + index['dc:title'] + r''' t9 
        on(t1.subject = t2.subject and t1.object = "bench:Inproceedings"
        and t2.subject = t3.subject and t3.subject = t4.subject and t4.subject = t5.subject and t5.subject = t6.subject and t6.subject = t7.subject
        and t7.subject = t8.subject and t8.subject = t9.subject) left join ''' + index['bench:abstract'] + r''' t10 on(t9.subject = t10.subject)  
        ORDER BY t9.object LIMIT 100000000;''')
    queries.append(query9)
    
        #Q9.5
    query9 = (r'select STRAIGHT_JOIN  * from ' +  index['bench:booktitle'] + r' t1 join ' + 
              index['dc:creator'] + r' t2 join ' +index['dcterms:issued'] + r' t3 join ' +  index['rdf:type'] + r''' t4 
       join ''' + index['dcterms:partOf'] + r''' t5 join ''' + index['rdfs:seeAlso'] + r' t6 join ' + index['swrc:pages'] + r' t7 join ' + index['foaf:homepage'] + ' t8 ' + r'''
       join ''' + index['dc:title'] + r''' t9 
        on(t1.subject = t2.subject and t1.object = "bench:Inproceedings"
        and t2.subject = t3.subject and t3.subject = t4.subject and t4.subject = t5.subject and t5.subject = t6.subject and t6.subject = t7.subject
        and t7.subject = t8.subject and t8.subject = t9.subject) left join ''' + index['bench:abstract'] + r''' t10 on(t9.subject = t10.subject)  
        ORDER BY t9.object LIMIT 100000000;''')
    queries.append(query9)
    
#Q9.6
    query9 = (r'select STRAIGHT_JOIN  * from ' + index['rdfs:seeAlso'] + r' t1 join ' + 
              index['dc:creator'] + r' t2 join ' +index['dcterms:issued'] + r' t3 join ' +  index['rdf:type'] + r''' t4 
       join ''' + index['dcterms:partOf'] + r''' t5 join ''' +  index['bench:booktitle'] + r' t6 join ' + index['swrc:pages'] + r' t7 join ' + index['foaf:homepage'] + ' t8 ' + r'''
       join ''' + index['dc:title'] + r''' t9 
        on(t1.subject = t2.subject and t1.object = "bench:Inproceedings"
        and t2.subject = t3.subject and t3.subject = t4.subject and t4.subject = t5.subject and t5.subject = t6.subject and t6.subject = t7.subject
        and t7.subject = t8.subject and t8.subject = t9.subject) left join ''' + index['bench:abstract'] + r''' t10 on(t9.subject = t10.subject)  
        ORDER BY t9.object LIMIT 100000000;''')
    queries.append(query9)



    


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
        print("Querying tables with suffix " + tablesuffix)

    logfilename = opts['-l']   
    
    issueQuery(dataset, logfilename, tablesuffix)

if __name__ == '__main__':
    main()

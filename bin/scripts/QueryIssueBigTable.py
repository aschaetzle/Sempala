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


    # single pattern, Objekt gebunden, sehr selektiv
    query1 = r'select distinct id from ' + tablename + r' where dc_title = "\"Journal 1 \(1940\)\"\^\^xsd:string";'
    queries.append(query1)    
    

    # single pattern, Subjekt gebunden, sehr selektiv
    query2 = r'select distinct dc_title from ' + tablename + r''' 
    where id ="<http://localhost/publications/journals/Journal1/1940>"
    and dc_title is not null ;'''
    queries.append(query2)     

    # single pattern, subjekt ungebunden, unselektiv
    # inproceedings noch mehr
    query3 = r'select distinct id  from ' + tablename + r' where rdf_type = "bench:Article";'
    queries.append(query3)  

    # Fast Q1 als join 
    query4 = r'select distinct id, rdf_type from ' + tablename + r''' where dc_title = "\"Journal 1 \(1940\)\"\^\^xsd:string" ;'''
    queries.append(query4)  


    # Fast Q1 als join mit gefiltertem subquery
    query5 =r'select distinct id, rdf_type from ' + tablename + r'''  where dc_title = "\"Journal 1 \(1940\)\"\^\^xsd:string" ;'''
    queries.append(query5)  
    
    
    # Deep Chained joins no filter
    query6 = r'''select distinct t1.dcterms_references, t2.id, t2.rdf__1, t3.id, t3.dc_creator, t4.id, t4.foaf_name from
    (select dcterms_references from ''' + tablename + r''' where dcterms_references is not null) t1  join 
    (select id, rdf__1 from  ''' + tablename + r''' where id is not null and rdf__1 is  not null) t2 on(t1.dcterms_references = t2.id) join 
    (select id, dc_creator from ''' + tablename + r''' where id is not null and dc_creator is  not null) t3 on(t2.rdf__1 = t3.id) join 
    (select id, foaf_name from ''' + tablename + r''' where id is  not null and foaf_name is  not null) t4 on (t3.dc_creator = t4.id ) '''
    queries.append(query6) 
    
 
    # Deep Chained joins with filter
    query7 = r'''select distinct t1.dcterms_references, t2.id, t2.rdf__1, t3.id, t3.dc_creator, t4.id, t4.foaf_name from
(select id, dcterms_references from ''' + tablename + r''' where id="<http://localhost/publications/inprocs/Proceeding567/2001/Inproceeding33402>" ) t1  join 
(select id, rdf__1 from  ''' + tablename + r'''  where id is not null and rdf__1 is  not null)
     t2 on(t1.dcterms_references = t2.id) join 
(select id, dc_creator from ''' + tablename + r''' where id is not null and dc_creator is  not null) t3 on(t2.rdf__1 = t3.id) join 
(select id, foaf_name from ''' + tablename + r''' where id is  not null and foaf_name is  not null) t4 on (t3.dc_creator = t4.id )'''
    queries.append(query7) 
    
    # Wide star joins
    query8 = r'select distinct id, rdf_type, swrc_pages, dc_creator, dc_title, swrc_month, swrc_note from '+tablename+ r'''
    where id is not null and rdf_type is not null and swrc_pages is not null and swrc_month is not null and  swrc_note is not null ;'''
    queries.append(query8) 
    
    
    # angelehnt an Q2 mit ORDER BY 
    query9 = r'''select distinct id, rdf_type, dc_creator, bench_booktitle, dc_title, dcterms_partOf,  rdfs_seeAlso, 
    swrc_pages, foaf_homepage, dcterms_issued, bench_abstract from ''' + tablename + r''' 
    WHERE rdf_type = "bench:Inproceedings" and  dc_creator is not null and bench_booktitle is not null and  dc_title is not null and  dcterms_partOf is not null 
    and rdfs_seeAlso is not null and swrc_pages is not null and foaf_homepage is not null and dcterms_issued is not null 
    ORDER BY dcterms_issued LIMIT 100000000 ;''' 
    queries.append(query9)
# 

# 
    # angelehnt an SP2Bench Q10
    
    predicates = []
    impala.queryToStdOut('Describe bigtable_text;')
    for line in impala.getResults():
        predicates.append(line.split()[0])

    query10 = r'''select * from ''' + tablename + r''' where '''
    first = True
    for pred in predicates:
        if first:
            first = False
            query10 += pred +r''' = "<http://localhost/persons/Paul_Erdoes>" '''
        else: 
           query10 += ' or '+ pred +r''' = "<http://localhost/persons/Paul_Erdoes>" '''
    query10 += r'''; '''
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
    
    issueQueryBigTable(dataset, tablename, logfile)


if __name__ == '__main__':
    main()

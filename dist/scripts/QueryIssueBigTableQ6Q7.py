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
    names = ['Q6.1', 'Q6.2', 'Q6.3', 'Q6.4', 'Q7.1', 'Q7.2', 'Q7.3', 'Q7.4,', 'Q6.1[partitioned]', 'Q6.2[partitioned]', 'Q6.3[partitioned]', 'Q6.4[partitioned]', 'Q7.1[partitioned]', 'Q7.2[partitioned]', 'Q7.3[partitioned]', 'Q7.4[partitioned]']

    
    # Deep Chained joins no filter
    query6 = r'''select distinct t1.dcterms_references, t2.id, t2.rdf__1, t3.id, t3.dc_creator, t4.id, t4.foaf_name from
    (select dcterms_references from ''' + tablename + r''' where dcterms_references is not null) t1  join 
    (select id, rdf__1 from  ''' + tablename + r''' where id is not null and rdf__1 is  not null) t2 on(t1.dcterms_references = t2.id) join 
    (select id, dc_creator from ''' + tablename + r''' where id is not null and dc_creator is  not null) t3 on(t2.rdf__1 = t3.id) join 
    (select id, foaf_name from ''' + tablename + r''' where id is  not null and foaf_name is  not null) t4 on (t3.dc_creator = t4.id ) '''
    queries.append(query6) 
    
    
    query6 = r'''select distinct t1.dcterms_references, t2.id, t2.rdf__1, t3.id, t3.dc_creator, t4.id, t4.foaf_name from
(select id, dc_creator from ''' + tablename + r''' where id is not null and dc_creator is  not null) t3 join
(select id, rdf__1 from  ''' + tablename + r''' where id is not null and rdf__1 is  not null) t2 on(t2.rdf__1 = t3.id) join
(select dcterms_references from ''' + tablename + r''' where dcterms_references is not null) t1 on(t1.dcterms_references = t2.id) join
(select id, foaf_name from ''' + tablename + r''' where id is  not null and foaf_name is  not null) t4 on (t3.dc_creator = t4.id ) '''
    queries.append(query6) 
    
    
    query6 = r'''select distinct t1.dcterms_references, t2.id, t2.rdf__1, t3.id, t3.dc_creator, t4.id, t4.foaf_name from
(select id, rdf__1 from  ''' + tablename + r''' where id is not null and rdf__1 is  not null) t2  join
(select id, dc_creator from ''' + tablename + r''' where id is not null and dc_creator is  not null) t3 on(t2.rdf__1 = t3.id) join
(select dcterms_references from ''' + tablename + r''' where dcterms_references is not null) t1 on(t1.dcterms_references = t2.id) join
(select id, foaf_name from ''' + tablename + r''' where id is  not null and foaf_name is  not null) t4 on (t3.dc_creator = t4.id ) '''
    queries.append(query6) 
    
    query6 = r'''select distinct t1.dcterms_references, t2.id, t2.rdf__1, t3.id, t3.dc_creator, t4.id, t4.foaf_name from
(select id, foaf_name from ''' + tablename + r''' where id is  not null and foaf_name is  not null) t4 join 
(select id, dc_creator from ''' + tablename + r''' where id is not null and dc_creator is  not null) t3  on(t3.dc_creator = t4.id ) join
(select id, rdf__1 from  ''' + tablename + r''' where id is not null and rdf__1 is  not null)    t2 on(t2.rdf__1 = t3.id) join
(select dcterms_references from ''' + tablename + r''' where dcterms_references is not null) t1  on(t1.dcterms_references = t2.id) '''
    queries.append(query6) 
    
    
    
 
    # Deep Chained joins with filter
    query7 = r'''select distinct t1.dcterms_references, t2.id, t2.rdf__1, t3.id, t3.dc_creator, t4.id, t4.foaf_name from
(select id, dcterms_references from ''' + tablename + r''' where id="<http://localhost/publications/inprocs/Proceeding567/2001/Inproceeding33402>" ) t1  join 
(select id, rdf__1 from  ''' + tablename + r'''  where id is not null and rdf__1 is  not null)
     t2 on(t1.dcterms_references = t2.id) join 
(select id, dc_creator from ''' + tablename + r''' where id is not null and dc_creator is  not null) t3 on(t2.rdf__1 = t3.id) join 
(select id, foaf_name from ''' + tablename + r''' where id is  not null and foaf_name is  not null) t4 on (t3.dc_creator = t4.id )'''
    queries.append(query7) 
        
        
    query7 = r'''select distinct t1.dcterms_references, t2.id, t2.rdf__1, t3.id, t3.dc_creator, t4.id, t4.foaf_name from
(select id, dc_creator from ''' + tablename + r''' where dc_creator is not null) t3 join
(select id, rdf__1 from  ''' + tablename + r''' where id is not null and rdf__1 is  not null) t2 on(t2.rdf__1 = t3.id) join
(select id, dcterms_references from ''' + tablename + r''' where id="<http://localhost/publications/inprocs/Proceeding567/2001/Inproceeding33402>" and dcterms_references is not null) t1 on(t1.dcterms_references = t2.id) join
(select id, foaf_name from ''' + tablename + r''' where id is  not null and foaf_name is  not null) t4 on (t3.dc_creator = t4.id )'''
    queries.append(query7) 
        
        
    query7 = r'''select distinct t1.dcterms_references, t2.id, t2.rdf__1, t3.id, t3.dc_creator, t4.id, t4.foaf_name from
(select id, rdf__1 from  ''' + tablename + r''' where id is not null and rdf__1 is not null) t2 join
(select id, dc_creator from ''' + tablename + r''' where dc_creator is not null) t3  on(t2.rdf__1 = t3.id) join
(select id, foaf_name from ''' + tablename + r''' where id is  not null and foaf_name is  not null) t4 on (t3.dc_creator = t4.id) join
(select id, dcterms_references from ''' + tablename + r''' where id="<http://localhost/publications/inprocs/Proceeding567/2001/Inproceeding33402>" and dcterms_references is not null)
 t1 on(t1.dcterms_references = t2.id)'''
    queries.append(query7) 
        
        
    query7 = r'''select distinct t1.dcterms_references, t2.id, t2.rdf__1, t3.id, t3.dc_creator, t4.id, t4.foaf_name from
(select id, foaf_name from ''' + tablename + r''' where id is  not null and foaf_name is  not null) t4 join
(select id, dc_creator from ''' + tablename + r''' where dc_creator is not null) t3 on (t3.dc_creator = t4.id) join
(select id, rdf__1 from  ''' + tablename + r''' where id is not null and rdf__1 is not null) t2 on(t2.rdf__1 = t3.id) join
(select id, dcterms_references from ''' + tablename + r''' where id="<http://localhost/publications/inprocs/Proceeding567/2001/Inproceeding33402>" and dcterms_references is not null)
 t1 on(t1.dcterms_references = t2.id)'''
    queries.append(query7) 
    
    # partitioned joins 
    query6 = r'''select distinct STRAIGHT_JOIN t1.dcterms_references, t2.id, t2.rdf__1, t3.id, t3.dc_creator, t4.id, t4.foaf_name from
    (select dcterms_references from ''' + tablename + r''' where dcterms_references is not null) t1  join [shuffle] 
    (select id, rdf__1 from  ''' + tablename + r''' where id is not null and rdf__1 is  not null) t2 on(t1.dcterms_references = t2.id) join [shuffle]
    (select id, dc_creator from ''' + tablename + r''' where id is not null and dc_creator is  not null) t3 on(t2.rdf__1 = t3.id) join  [shuffle]
    (select id, foaf_name from ''' + tablename + r''' where id is  not null and foaf_name is  not null) t4 on (t3.dc_creator = t4.id ) '''
    queries.append(query6) 
    
    
    query6 = r'''select distinct STRAIGHT_JOIN t1.dcterms_references, t2.id, t2.rdf__1, t3.id, t3.dc_creator, t4.id, t4.foaf_name from
(select id, dc_creator from ''' + tablename + r''' where id is not null and dc_creator is  not null) t3 join [shuffle]
(select id, rdf__1 from  ''' + tablename + r''' where id is not null and rdf__1 is  not null) t2 on(t2.rdf__1 = t3.id) join [shuffle]
(select dcterms_references from ''' + tablename + r''' where dcterms_references is not null) t1 on(t1.dcterms_references = t2.id) join [shuffle]
(select id, foaf_name from ''' + tablename + r''' where id is  not null and foaf_name is  not null) t4 on (t3.dc_creator = t4.id ) '''
    queries.append(query6) 
    
    
    query6 = r'''select distinct STRAIGHT_JOIN t1.dcterms_references, t2.id, t2.rdf__1, t3.id, t3.dc_creator, t4.id, t4.foaf_name from
(select id, rdf__1 from  ''' + tablename + r''' where id is not null and rdf__1 is  not null) t2  join [shuffle]
(select id, dc_creator from ''' + tablename + r''' where id is not null and dc_creator is  not null) t3 on(t2.rdf__1 = t3.id) join [shuffle]
(select dcterms_references from ''' + tablename + r''' where dcterms_references is not null) t1 on(t1.dcterms_references = t2.id) join[shuffle]
(select id, foaf_name from ''' + tablename + r''' where id is  not null and foaf_name is  not null) t4 on (t3.dc_creator = t4.id ) '''
    queries.append(query6) 
    
    query6 = r'''select distinct STRAIGHT_JOIN t1.dcterms_references, t2.id, t2.rdf__1, t3.id, t3.dc_creator, t4.id, t4.foaf_name from
(select id, foaf_name from ''' + tablename + r''' where id is  not null and foaf_name is  not null) t4 join [shuffle]
(select id, dc_creator from ''' + tablename + r''' where id is not null and dc_creator is  not null) t3  on(t3.dc_creator = t4.id ) join [shuffle]
(select id, rdf__1 from  ''' + tablename + r''' where id is not null and rdf__1 is  not null)    t2 on(t2.rdf__1 = t3.id) join [shuffle]
(select dcterms_references from ''' + tablename + r''' where dcterms_references is not null) t1  on(t1.dcterms_references = t2.id) '''
    queries.append(query6) 
    
        # Deep Chained joins with filter
    query7 = r'''select distinct STRAIGHT_JOIN t1.dcterms_references, t2.id, t2.rdf__1, t3.id, t3.dc_creator, t4.id, t4.foaf_name from
(select id, dcterms_references from ''' + tablename + r''' where id="<http://localhost/publications/inprocs/Proceeding567/2001/Inproceeding33402>" ) t1  join  [shuffle]
(select id, rdf__1 from  ''' + tablename + r'''  where id is not null and rdf__1 is  not null)
     t2 on(t1.dcterms_references = t2.id) join  [shuffle]
(select id, dc_creator from ''' + tablename + r''' where id is not null and dc_creator is  not null) t3 on(t2.rdf__1 = t3.id) join [shuffle]
(select id, foaf_name from ''' + tablename + r''' where id is  not null and foaf_name is  not null) t4 on (t3.dc_creator = t4.id )'''
    queries.append(query7) 
        
        
    query7 = r'''select distinct STRAIGHT_JOIN t1.dcterms_references, t2.id, t2.rdf__1, t3.id, t3.dc_creator, t4.id, t4.foaf_name from
(select id, dc_creator from ''' + tablename + r''' where dc_creator is not null) t3 join [shuffle]
(select id, rdf__1 from  ''' + tablename + r''' where id is not null and rdf__1 is  not null) t2 on(t2.rdf__1 = t3.id) join [shuffle]
(select id, dcterms_references from ''' + tablename + r''' where id="<http://localhost/publications/inprocs/Proceeding567/2001/Inproceeding33402>" and dcterms_references is not null) t1 on(t1.dcterms_references = t2.id) join [shuffle]
(select id, foaf_name from ''' + tablename + r''' where id is  not null and foaf_name is  not null) t4 on (t3.dc_creator = t4.id )'''
    queries.append(query7) 
        
        
    query7 = r'''select distinct STRAIGHT_JOIN t1.dcterms_references, t2.id, t2.rdf__1, t3.id, t3.dc_creator, t4.id, t4.foaf_name from
(select id, rdf__1 from  ''' + tablename + r''' where id is not null and rdf__1 is not null) t2 join [shuffle]
(select id, dc_creator from ''' + tablename + r''' where dc_creator is not null) t3  on(t2.rdf__1 = t3.id) join [shuffle]
(select id, foaf_name from ''' + tablename + r''' where id is  not null and foaf_name is  not null) t4 on (t3.dc_creator = t4.id) join [shuffle]
(select id, dcterms_references from ''' + tablename + r''' where id="<http://localhost/publications/inprocs/Proceeding567/2001/Inproceeding33402>" and dcterms_references is not null)
 t1 on(t1.dcterms_references = t2.id)'''
    queries.append(query7) 
        
        
    query7 = r'''select distinct STRAIGHT_JOIN t1.dcterms_references, t2.id, t2.rdf__1, t3.id, t3.dc_creator, t4.id, t4.foaf_name from
(select id, foaf_name from ''' + tablename + r''' where id is  not null and foaf_name is  not null) t4 join [shuffle]
(select id, dc_creator from ''' + tablename + r''' where dc_creator is not null) t3 on (t3.dc_creator = t4.id) join [shuffle]
(select id, rdf__1 from  ''' + tablename + r''' where id is not null and rdf__1 is not null) t2 on(t2.rdf__1 = t3.id) join [shuffle]
(select id, dcterms_references from ''' + tablename + r''' where id="<http://localhost/publications/inprocs/Proceeding567/2001/Inproceeding33402>" and dcterms_references is not null)
 t1 on(t1.dcterms_references = t2.id)'''
    queries.append(query7) 


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

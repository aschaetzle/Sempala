#!/usr/bin/env python

# author: Antony Neu
# Python version: 2.5


# Loads a partitioned Bigtable 

import os
from string import Template
import subprocess
from sys import argv
from Util import replaceSpecialChars
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

copy_bigtable_query = r"""

DROP TABLE IF EXISTS bigtable_parquet; 
CREATE TABLE bigtable_parquet LIKE bigtable_ext STORED AS PARQUETFILE;
set PARQUET_COMPRESSION_CODEC=snappy;
insert overwrite table bigtable_parquet select * FROM bigtable_ext;
 COMPUTE STATS bigtable_parquet;

"""

create_triplestore_table_query = Template(r"""
DROP TABLE IF EXISTS triplestore_ext;
CREATE EXTERNAL TABLE triplestore_ext
(
    ID STRING,
    predicate STRING,
    object STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
LOCATION "/user/neua/${dataset}";

DROP TABLE IF EXISTS triplestore_parquet;
CREATE TABLE triplestore_parquet LIKE triplestore_ext STORED AS PARQUETFILE;
insert overwrite table triplestore_parquet select * from triplestore_ext;

COMPUTE STATS triplestore_parquet;

DROP TABLE IF EXISTS triplestore_ext;
""")



def loadAllDatasets(usemr, partitioned_table):
    print('Loading all datasets')
    datasets = ['berlin_500k', 'berlin_1000k','berlin_1500k','berlin_2000k','berlin_2500k', 'berlin_3000k']
    for ds in datasets:
        loadDataset(ds, usemr, partitioned_table)

def loadDataset(dataset, usemr, partitioned_table):
    # triple store
    print('Loading rdf file dataset ' + dataset)
    
    if usemr:
        os.popen('hadoop fs -rmr ./'+dataset)
    print('Converting to triple table csv file')
    if usemr:
        os.popen('hadoop jar ./RDFConverter.jar com.antony_neu.thesis.RDFToCSV.RDFConverter -f 1 /data/berlin/'+dataset+'.nt  10 '+dataset)
        os.popen('hadoop fs -chmod -R 777 '+dataset)    
    impala = ImpalaWrapper(dataset+'_bigtable')
    impala.clearDatabase()

    impala.query(create_triplestore_table_query.substitute(dataset=dataset))

    # BigTable conversion
    if usemr:
        os.popen('hadoop fs -rmr ./' + dataset)
        os.popen('hadoop fs -rmr ./' + dataset + '_preprocessing')
        os.popen('hadoop jar ./RDFConverter.jar com.antony_neu.thesis.RDFToCSV.RDFConverter -f 2 /data/berlin/' + dataset + '.nt 10 ' + dataset)
        os.popen('hadoop fs -chmod -R 777 '+dataset)    
    
    # load external 
    # create predicate list from preprocessing
    proc = subprocess.Popen('hadoop fs -cat '+dataset+'_preprocessing/part-r-*', stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    predicates = proc.stdout.readlines()
    predicates.sort()
    predicates = map(replaceSpecialChars, predicates)

    querystring = r"""
        DROP TABLE IF EXISTS bigtable_ext;
        CREATE EXTERNAL TABLE  bigtable_ext
        (
            ID STRING"""

    
    for pred in predicates:
        name = pred.split()[0].strip()
        typen = pred.split()[1].strip()
        if typen == 'xsd_integer' or typen == replaceSpecialChars('<http://www.w3.org/2001/XMLSchema#integer>'):
            querystring += r""", """+name+r""" INT"""
        elif typen == replaceSpecialChars('<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/USD>'):
            querystring += r""", """+name+r""" DOUBLE"""
        else:
            print("typen was "+typen)
            querystring += r""", """+name+r""" STRING"""
    
    
    querystring += r"""            )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
    LOCATION "/user/neua/${dataset}/";"""
    query = Template(querystring)

    impala = ImpalaWrapper(dataset+"_bigtable")

    impala.query(query.substitute(dataset=dataset))
    impala.query(copy_bigtable_query)
    
    # load partitioned table

    # create predicate list from preprocessing
    if partitioned_table:
        proc = subprocess.Popen('hadoop fs -cat ' + dataset + '_preprocessing/part-r-*', stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        predicates = map(replaceSpecialChars, proc.stdout.readlines())
        predicates.sort()
        querystring = r"""
            DROP TABLE IF EXISTS bigtable_partitioned;
            set PARQUET_COMPRESSION_CODEC=snappy;
            CREATE TABLE  bigtable_partitioned
            (
                ID STRING"""
     
         
        for pred in predicates:
            if(not(pred.split()[0].strip() == 'http___www_w3_org_1999_02_22_rdf_syntax_ns_type_')):
                name = pred.split()[0].strip()
                typen = pred.split()[1].strip()
                if typen == 'xsd_integer' or typen == replaceSpecialChars('<http://www.w3.org/2001/XMLSchema#integer>'):
                    querystring += r""", """+name+r""" INT"""
                elif typen == replaceSpecialChars('<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/USD>'):
                    querystring += r""", """+name+r""" DOUBLE"""
                else:
                    print("typen was "+typen)
                    querystring += r""", """+name+r""" STRING"""
         
         
        querystring += r"""            )
        PARTITIONED BY (http___www_w3_org_1999_02_22_rdf_syntax_ns_type_ STRING)
        STORED AS PARQUETFILE ;"""
        query = Template(querystring)
        impala = ImpalaWrapper(dataset + "_bigtable")
        impala.query(query.substitute(dataset=dataset))
     
        query = r"""INSERT INTO bigtable_partitioned partition(http___www_w3_org_1999_02_22_rdf_syntax_ns_type_) 
        SELECT ID """
        for pred in predicates:
            if(not(pred.split()[0].strip() == 'http___www_w3_org_1999_02_22_rdf_syntax_ns_type_')):
                name = pred.split()[0].strip()
                query += r""", """ + name 
        query += r""", http___www_w3_org_1999_02_22_rdf_syntax_ns_type_ FROM bigtable_ext ; COMPUTE STATS bigtable_partitioned; DROP TABLE IF EXISTS bigtable_ext;"""
        impala.query(query)
    
    
    


def main():
    opts = getOpts(argv)
    usemr = True
    partitioned_table = True
    try: 
        opts['-wmr']
        usemr = False
    except:
        usemr = True
        print("Using mapreduce jobs")
    try: 
        opts['-omitparttable']
        partitioned_table = False
    except:
        partitioned_table = True
        print("Omitting partitioned table")
    try:
        dataset = opts['-d']
    except KeyError:
        print("no dataset")
        loadAllDatasets(usemr,partitioned_table)
    
    # load single dataset
    loadDataset(dataset, usemr,partitioned_table)
        

if __name__ == "__main__":
    main()

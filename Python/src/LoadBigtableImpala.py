#!/usr/bin/env python

# author: Antony Neu
# Python version: 2.5


# Loads a partitioned Bigtable 

import os
from string import Template
import subprocess
from sys import argv
import time 
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


def loadAllDatasets(datasetname, usemr, partitioned_table):
    print('Loading all datasets')
    if datasetname == 'sp2bench':
        datasets = ['dblp1M', 'dblp10M', 'dblp25M', 'dblp50M', 'dblp100M', 'dblp200M', 'dblp300M', 'dblp400M', 'dblp500M']
    elif datasetname == 'lubm':
        datasets = ['lubm500', 'lubm1000', 'lubm1500', 'lubm2000', 'lubm2500', 'lubm3000']
    else:
        datasets = ['berlin_500k', 'berlin_1000k', 'berlin_1500k', 'berlin_2000k', 'berlin_2500k', 'berlin_3000k']
    
    
    for ds in datasets:
        loadDataset(ds)

def loadDataset(datasetname, dataset, usemr, partitioned_table):
    logfile = datasetname+"_loading_impala.log"
    dbname = dataset + "_bigtable_impala"
    
    start = time.time()  
    
    # define path
    if datasetname == 'sp2bench':
        pathname = '/data/sp2bench/' + dataset + '.n3'
    elif datasetname == 'lubm':
        pathname = '/data/lubm/' + dataset + '_r.nt'
    else:
        pathname = '/data/berlin/' + dataset + '.nt'
    
    # triple store
    print('Loading rdf file dataset ' + dataset)
    
    if usemr:
        os.popen('hadoop fs -rmr ./' + dataset)
        print('Converting to triple table csv file')
        os.popen('hadoop jar ./RDFConverter.jar com.antony_neu.thesis.RDFToCSV.RDFConverter -f 1 ' + pathname + ' 10 ' + dataset)
        # change permissions of created folder
        os.popen('hadoop fs -chmod -R 777 ' + dataset)    
    impala = ImpalaWrapper(dbname)
    impala.clearDatabase()
    impala.query(create_triplestore_table_query.substitute(dataset=dataset))
    
    # end triple store

    # BigTable conversion
    if usemr:
        os.popen('hadoop fs -rmr ./' + dataset)
        os.popen('hadoop fs -rmr ./' + dataset + '_lubm' + '_preprocessing') 
        os.popen('hadoop fs -rmr ./' + dataset + '_preprocessing')
        if datasetname == 'lubm':
            os.popen('hadoop jar ./RDFConverter.jar com.antony_neu.thesis.RDFToCSV.RDFConverter --lubm -f 2 ' + pathname + ' 10 ' + dataset)
        else :
            os.popen('hadoop jar ./RDFConverter.jar com.antony_neu.thesis.RDFToCSV.RDFConverter -f 2 ' + pathname + ' 10 ' + dataset)
        os.popen('hadoop fs -chmod -R 777 ' + dataset)    
    
    
    # load external property table
    # create predicate list from preprocessing
    proc = subprocess.Popen('hadoop fs -cat ' + dataset + '_preprocessing/part-r-*', stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    predicates = map(replaceSpecialChars, proc.stdout.readlines())
    predicates.sort()
    querystring = r"""
        DROP TABLE IF EXISTS bigtable_ext;
        CREATE EXTERNAL TABLE  bigtable_ext
        (
            ID STRING"""

    
    for pred in predicates:
        name = pred.split()[0].strip()
        typen = pred.split()[1].strip()
        if 'integer' in typen or typen == 'xsd_integer' or typen == replaceSpecialChars('<http://www.w3.org/2001/XMLSchema#integer>'):
            querystring += r""", """ + name + r""" INT"""
        elif 'usd' in typen or typen == replaceSpecialChars('<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/USD>'):
            querystring += r""", """ + name + r""" DOUBLE"""
        else:
            print("typen was " + typen)
            querystring += r""", """ + name + r""" STRING"""
    
    
    
    querystring += r"""            )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
    LOCATION "/user/neua/${dataset}/";"""
    query = Template(querystring)

    impala = ImpalaWrapper(dbname)

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
            if(not(pred.split()[0].strip() == 'rdf_type')):
                name = pred.split()[0].strip()
                typen = pred.split()[1].strip()
                if 'integer' in typen or typen == 'xsd_integer' or typen == replaceSpecialChars('<http://www.w3.org/2001/XMLSchema#integer>'):
                    querystring += r""", """ + name + r""" INT"""
                elif 'usd' in typen or typen == replaceSpecialChars('<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/USD>'):
                    querystring += r""", """ + name + r""" DOUBLE"""
                else:
                    querystring += r""", """ + name + r""" STRING"""
         
        querystring += r"""            )
        PARTITIONED BY (rdf_type STRING)
        STORED AS PARQUETFILE ;"""
        query = Template(querystring)
        impala = ImpalaWrapper(dbname)
        impala.query(query.substitute(dataset=dataset))
     
        query = r"""INSERT INTO bigtable_partitioned partition(rdf_type) 
        SELECT ID """
        for pred in predicates:
            if(not(pred.split()[0].strip() == 'rdf_type')):
                name = pred.split()[0].strip()
                query += r""", """ + name 
        query += r""", rdf_type FROM bigtable_ext ; COMPUTE STATS bigtable_partitioned; DROP TABLE IF EXISTS bigtable_ext;"""
        impala.query(query)
    
        elapsed = time.time() -start 
        with open(logfile, "a") as myfile:
            myfile.write(str(elapsed)+" s\n")
    
    


def main():
    opts = getOpts(argv)
    try: 
        opts['-withoutmr']
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
        datasetname = opts['-dn']
    except KeyError:
        print()
        exit(404)
    try:
        dataset = opts['-d']
    except KeyError:
        print("no dataset")
        loadAllDatasets(datasetname, usemr, partitioned_table)
    
    # load single dataset
    loadDataset(datasetname, dataset, usemr, partitioned_table)
        

if __name__ == "__main__":
    main()

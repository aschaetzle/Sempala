#!/usr/bin/env python

# author: Antony Neu
# Python version: 2.5


# Loads a partitioned Bigtable 

import os
from string import Template
import subprocess
from sys import argv

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



def loadAllDatasets():
    print('Loading all datasets')
    datasets = ['dblp250K', 'dblp1M', 'dblp10M', 'dblp25M', 'dblp50M', 'dblp100M', 'dblp200M']
    for ds in datasets:
        loadDataset(ds)

def loadDataset(dataset):
    # triple store
    print('Loading rdf file dataset ' + dataset)
    
    os.popen('hadoop fs -rmr ./'+dataset)
    print('Converting to triple table csv file')
    os.popen('hadoop jar ./RDFConverter.jar com.antony_neu.thesis.RDFToCSV.RDFConverter -f 1 /data/sp2bench/'+dataset+'.n3  10 '+dataset)
    # change permissions of created folder
    os.popen('hadoop fs -chmod -R 777 '+dataset)    
    impala = ImpalaWrapper(dataset+'_bigtable')

    impala.query(create_triplestore_table_query.substitute(dataset=dataset))

    # BigTable conversion
    os.popen('hadoop fs -rmr ./' + dataset)
    os.popen('hadoop fs -rmr ./' + dataset + '_preprocessing')
    os.popen('hadoop jar ./RDFConverter.jar com.antony_neu.thesis.RDFToCSV.RDFConverter -f 2 /data/sp2bench/' + dataset + '.n3 10 ' + dataset)
    os.popen('hadoop fs -chmod -R 777 '+dataset)    
    
    # load external 
    # create predicate list from preprocessing
    proc = subprocess.Popen('hadoop fs -cat '+dataset+'_preprocessing/part-r-*', stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    predicates = map(lambda x: x.replace(':','_').strip(), proc.stdout.readlines())
    predicates.sort()
    querystring = r"""
        DROP TABLE IF EXISTS bigtable_ext;
        CREATE EXTERNAL TABLE  bigtable_ext
        (
            ID STRING"""

    
    for pred in predicates:
        name = pred.split()[0].strip()
        typen = pred.split()[1].strip()
        if typen == 'xsd_integer':
            querystring += r""", """+name+r""" INT"""
        else:
            querystring += r""", """+name+r""" STRING"""
    
    
    querystring += r"""            )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
    LOCATION "/user/neua/${dataset}/";"""
    query = Template(querystring)

    impala = ImpalaWrapper(dataset+"_bigtable")
    impala.clearDatabase()
    impala.query(query.substitute(dataset=dataset))
    impala.query(copy_bigtable_query)
    
    # load partitioned table

    # create predicate list from preprocessing
    proc = subprocess.Popen('hadoop fs -cat ' + dataset + '_preprocessing/part-r-*', stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    predicates = map(lambda x: x.replace(':', '_').strip(), proc.stdout.readlines())
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
            if typen == 'xsd_integer':
                querystring += r""", """ + name + r""" INT"""
            else:
                querystring += r""", """ + name + r""" STRING"""
    
    
    querystring += r"""            )
    PARTITIONED BY (rdf_type STRING)
    STORED AS PARQUETFILE ;"""
    query = Template(querystring)
    impala = ImpalaWrapper(dataset + "_bigtable")
    impala.query(query.substitute(dataset=dataset))

    query = r"""INSERT INTO bigtable_partitioned partition(rdf_type) 
    SELECT ID """
    for pred in predicates:
        if(not(pred.split()[0].strip() == 'rdf_type')):
            name = pred.split()[0].strip()
            query += r""", """ + name 
    query += r""", rdf_type FROM bigtable_ext ; COMPUTE STATS bigtable_partitioned; DROP TABLE IF EXISTS bigtable_ext;"""
    impala.query(query)
    
    
    
    


def main():
    opts = getOpts(argv)
    try:
        dataset = opts['-d']
    except KeyError:
        print("no dataset")
        loadAllDatasets()
    
    # load single dataset
    loadDataset(dataset)
        

if __name__ == "__main__":
    main()

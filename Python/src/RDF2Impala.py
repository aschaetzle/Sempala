#!/usr/bin/env python

# author: Antony Neu
# Python version: 2.5

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

create_index_table_query = Template(r"""DROP TABLE IF EXISTS vertical_index;
CREATE EXTERNAL TABLE vertical_index
(
	key STRING,
	name STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
LOCATION "/user/neua/${dataset}/index";
DROP TABLE IF EXISTS vertical_index_parquet;
CREATE TABLE vertical_index_parquet LIKE vertical_index STORED AS PARQUETFILE;
insert overwrite table vertical_index_parquet select * from vertical_index;""")


create_triplestore_table_query = Template(r"""
DROP TABLE IF EXISTS triplestore_ext;
CREATE EXTERNAL TABLE triplestore_ext
(
	subject STRING,
	predicate STRING,
	object STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
LOCATION "/user/neua/${dataset}";


DROP TABLE IF EXISTS triplestore_text;
CREATE TABLE triplestore_text
(
	subject STRING,
	predicate STRING,
	object STRING
);
insert overwrite table triplestore_text select * from triplestore_ext;

DROP TABLE IF EXISTS triplestore_parquet;
CREATE TABLE triplestore_parquet LIKE triplestore_text STORED AS PARQUETFILE;
insert overwrite table triplestore_parquet select * from triplestore_text;

DROP TABLE IF EXISTS triplestore_parquet_snappy;
set PARQUET_COMPRESSION_CODEC=snappy;
CREATE TABLE triplestore_parquet_snappy LIKE triplestore_text STORED AS PARQUETFILE;
insert overwrite table triplestore_parquet_snappy select * from triplestore_text;""")

create_data_table_query = Template(r"""
DROP TABLE IF EXISTS ${table}_ext ;
CREATE EXTERNAL TABLE  ${table}_ext
(
	subject STRING,
	object STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
LOCATION "/user/neua/${dataset}/tables/$table";

DROP TABLE IF EXISTS ${table}_text ;
CREATE TABLE  ${table}_text LIKE ${table}_ext;
insert overwrite table ${table}_text select * FROM ${table}_ext;


DROP TABLE IF EXISTS $table;

CREATE TABLE $table LIKE ${table}_text STORED AS PARQUETFILE;
insert overwrite table $table select * FROM ${table}_text ;

DROP TABLE IF EXISTS ${table}_snappy ; 

CREATE TABLE ${table}_snappy LIKE ${table}_text STORED AS PARQUETFILE;
set PARQUET_COMPRESSION_CODEC=snappy;
insert overwrite table ${table}_snappy select * FROM ${table}_text;

DROP TABLE IF EXISTS ${table}_gzip;

CREATE TABLE ${table}_gzip LIKE ${table}_text STORED AS PARQUETFILE;
set PARQUET_COMPRESSION_CODEC=gzip;
insert overwrite table ${table}_gzip select * FROM ${table}_text;

""")

copy_bigtable_query = r"""

DROP TABLE IF EXISTS bigtable_text ;
CREATE TABLE  bigtable_text LIKE bigtable_ext;
insert overwrite table bigtable_text select * FROM bigtable_ext;


DROP TABLE IF EXISTS bigtable_parquet;

CREATE TABLE bigtable_parquet LIKE bigtable_text STORED AS PARQUETFILE;
insert overwrite table bigtable_parquet select * FROM bigtable_text ;

DROP TABLE IF EXISTS bigtable_parquet_snappy ; 

CREATE TABLE bigtable_parquet_snappy LIKE bigtable_text STORED AS PARQUETFILE;
set PARQUET_COMPRESSION_CODEC=snappy;
insert overwrite table bigtable_parquet_snappy select * FROM bigtable_text;

DROP TABLE IF EXISTS bigtable_gzip;

CREATE TABLE bigtable_parquet_gzip LIKE bigtable_text STORED AS PARQUETFILE;
set PARQUET_COMPRESSION_CODEC=gzip;
insert overwrite table bigtable_parquet_gzip select * FROM bigtable_text;

"""

def loadAllDatasets(form):
    print('Loading all datasets')
    datasets=['dblp250K', 'dblp1M', 'dblp10M','dblp25M', 'dblp50M',  'dblp100M', 'dblp200M']
    for ds in datasets:
        loadDataset(ds, form)

def loadDataset(dataset, form):
    print('Loading rdf file dataset '+dataset)
    
    if(not form or form=="vertical"):
        print('Deleting output directory')
        os.popen('hadoop fs -rmr ./'+dataset)
    
        # run hadoop conversion
        print('Converting to csv file')
        os.popen('hadoop jar ./RDFConverter.jar com.antony_neu.thesis.RDFToCSV.RDFConverter -f 0 /data/sp2bench/'+dataset+'.n3  10 '+dataset)
    
        # change permissions of created folder
        os.popen('hadoop fs -chmod -R 777 '+dataset)    
    
        impala = ImpalaWrapper(dataset)
        impala.clearDatabase()
     
        # create index table
        print('Creating index table')
        impala.query(create_index_table_query.substitute(dataset=dataset))
     
        # query created index table
        query = 'SELECT key FROM vertical_index;'
        impala.queryToStdOut(query)
        tables = impala.getResults()
        for table in tables:
            table = table.strip()
            impala.query(create_data_table_query.substitute(table=table, dataset=dataset))
   
    if(not form or form=="tripletable"): 
        os.popen('hadoop fs -rmr ./'+dataset)
        print('Converting to triple table csv file')
        os.popen('hadoop jar ./RDFConverter.jar com.antony_neu.thesis.RDFToCSV.RDFConverter -f 1 /data/sp2bench/'+dataset+'.n3  10 '+dataset)
        # change permissions of created folder
        os.popen('hadoop fs -chmod -R 777 '+dataset)    
        impala = ImpalaWrapper(dataset+'_triplestore')
        impala.clearDatabase()
    
        impala.query(create_triplestore_table_query.substitute(dataset=dataset))

    if(not form or form=="bigtable"):
        # BigTable conversion
        os.popen('hadoop fs -rmr ./'+dataset)
        os.popen('hadoop fs -rmr ./'+dataset+'_preprocessing')
        os.popen('hadoop jar ./RDFConverter.jar com.antony_neu.thesis.RDFToCSV.RDFConverter -f 2 /data/sp2bench/'+dataset+'.n3 10 '+dataset)
    
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
            print typen
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
        
    


def main():
    opts = getOpts(argv)
    try: 
        form =  opts['-f']
    except KeyError: 
        print("No format specified")
    try:
        dataset = opts['-d']
    except KeyError:
        print("no dataset")
        loadAllDatasets(form)
    
    # load single dataset
    loadDataset(dataset, form)
        

if __name__ == "__main__":
    main()

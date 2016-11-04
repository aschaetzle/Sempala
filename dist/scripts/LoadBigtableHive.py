#!/usr/bin/env python

# author: Antony Neu
# Python version: 2.5


# Loads a partitioned Bigtable 

import os
from string import Template
import subprocess
from sys import argv, exit
from HiveWrapper import HiveWrapper
from Util import replaceSpecialChars
from ImpalaWrapper import ImpalaWrapper
import time 
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


create_triplestore_table_query = Template(r"""
DROP TABLE IF EXISTS triplestore_ext;
CREATE EXTERNAL TABLE triplestore_ext
(
    ID STRING,
    predicate STRING,
    object STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
LINES TERMINATED BY "\n"
LOCATION "/user/neua/${dataset}";
DROP TABLE IF EXISTS triplestore_parquet;
SET hive.exec.compress.output=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compression.type=BLOCK;
SET hive.exec.compress.intermediate = true;
set parquet.compression=snappy;
CREATE TABLE triplestore_parquet 
(
    ID STRING,
    predicate STRING,
    object STRING
)
ROW FORMAT SERDE "parquet.hive.serde.ParquetHiveSerDe"
STORED AS 
INPUTFORMAT "parquet.hive.DeprecatedParquetInputFormat"
OUTPUTFORMAT "parquet.hive.DeprecatedParquetOutputFormat";
set parquet.compression=snappy;
insert overwrite table triplestore_parquet select * from triplestore_ext;
DROP TABLE IF EXISTS triplestore_ext;
""")



def loadAllDatasets(datasetname, usemr):
    print('Loading all datasets')
    if datasetname == 'sp2bench':
        datasets = ['dblp1M', 'dblp10M', 'dblp25M', 'dblp50M', 'dblp100M', 'dblp200M', 'dblp300M', 'dblp400M', 'dblp500M']
    elif datasetname == 'lubm':
        #datasets = ['lubm500', 'lubm1000', 'lubm1500', 'lubm2000', 'lubm2500', 'lubm3000']
        datasets = ['lubm2000', 'lubm2500', 'lubm3000']
    else:
        datasets = ['berlin_500k', 'berlin_1000k', 'berlin_1500k', 'berlin_2000k', 'berlin_2500k', 'berlin_3000k']
    for ds in datasets:
        loadDataset(datasetname,ds, usemr)

def loadDataset(datasetname, dataset, usemr):
    print('Loadin dataset '+dataset)
    logfile = datasetname+"_loading.log"
    
    
    #pathname
    if datasetname == 'sp2bench':
        pathname = '/data/sp2bench/' + dataset + '.n3'
    elif datasetname == 'lubm':
        pathname = '/data/lubm/' + dataset + '_r.nt'
    else:
        pathname = '/data/berlin/' + dataset + '.nt'
    
    
    dbname = dataset+'_bigtable_hive'
    
    with open(logfile, "a") as myfile:
        myfile.write("Loading to "+dbname+"\n")
    
    
    start = time.time()    
    
    # triple store
    print('Loading rdf file dataset ' + dataset)
    
    if usemr:
        os.popen('hadoop fs -rmr ./'+dataset)
        print('Converting to triple table csv file')
        os.popen('hadoop jar ./RDFConverter.jar com.antony_neu.thesis.RDFToCSV.RDFConverter -f 1 '+pathname+'  10 '+dataset)
        # change permissions of created folder
        os.popen('hadoop fs -chmod -R 777 '+dataset)    
    # create query
    hive = HiveWrapper(dbname)

    hive.query(create_triplestore_table_query.substitute(dataset=dataset))

    impala = ImpalaWrapper(dbname)
    time.sleep(5)
    impala.query("INVALIDATE METADATA;")
    time.sleep(5)
    impala.query("COMPUTE STATS triplestore_parquet;")

    # end triple store

    # Property table conversion
    if usemr:
        os.popen('hadoop fs -rmr ./' + dataset)
        os.popen('hadoop fs -rmr ./' + dataset + '_preprocessing')
        os.popen('hadoop fs -rmr ./' + dataset + '_lubm' + '_preprocessing') 
        if datasetname == 'lubm':
            os.popen('hadoop jar ./RDFConverter.jar com.antony_neu.thesis.RDFToCSV.RDFConverter --lubm -f 2 ' + pathname + ' 10 ' + dataset)
        else :
            os.popen('hadoop jar ./RDFConverter.jar com.antony_neu.thesis.RDFToCSV.RDFConverter -f 2 '+pathname+' 10 ' + dataset)
        os.popen('hadoop fs -chmod -R 777 '+dataset)    
        
    # load external 
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
        if ('int' in typen) or  (typen == 'xsd_integer') or (typen == replaceSpecialChars('<http://www.w3.org/2001/XMLSchema#integer>')):
            querystring += r""", """+name+r""" INT"""
        elif (typen == replaceSpecialChars('<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/USD>') or ('USD' in typen)):
            querystring += r""", """+name+r""" DOUBLE"""
        else:
            print("typen was "+typen)
            querystring += r""", """+name+r""" STRING"""
    
    
    querystring += r"""            )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
    LOCATION "/user/neua/${dataset}/";"""
    query = Template(querystring)

    #run external
    hive = HiveWrapper(dbname)
    hive.query(query.substitute(dataset=dataset))
    
    
    # internal 
    querystring = r"""
        DROP TABLE IF EXISTS bigtable_parquet;
        SET mapred.output.compression.type=BLOCK;
        set hive.exec.compress.intermediate = true;
        SET hive.exec.compress.output=true;
        set parquet.compression=snappy;
        SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
        CREATE TABLE  bigtable_parquet
        (
            ID STRING"""
    for pred in predicates:
        name = pred.split()[0].strip()
        typen = pred.split()[1].strip()
        if ('int' in typen) or  (typen == 'xsd_integer') or (typen == replaceSpecialChars('<http://www.w3.org/2001/XMLSchema#integer>')):
            querystring += r""", """+name+r""" INT"""
        elif (typen == replaceSpecialChars('<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/USD>') or ('USD' in typen)):
            querystring += r""", """+name+r""" DOUBLE"""
        else:
            print("typen was "+typen)
            querystring += r""", """+name+r""" STRING"""
    
    querystring += r"""            ) 
ROW FORMAT SERDE "parquet.hive.serde.ParquetHiveSerDe"
STORED AS 
INPUTFORMAT "parquet.hive.DeprecatedParquetInputFormat"
OUTPUTFORMAT "parquet.hive.DeprecatedParquetOutputFormat";
set PARQUET_COMPRESSION_CODEC=snappy;
set parquet.compression=snappy;
insert overwrite table bigtable_parquet select * FROM bigtable_ext;
    """
    
    
    query = Template(querystring)
    hive = HiveWrapper(dbname)
    hive.query(query.substitute(dataset=dataset))
    
    
    impala = ImpalaWrapper(dbname)
    time.sleep(15)
    impala.query("INVALIDATE METADATA;")
    time.sleep(15)
    impala.query("COMPUTE STATS bigtable_parquet;")
    
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
        datasetname = opts['-dn']
    except KeyError:
        print()
        exit(404)
    try:
        dataset = opts['-dataset']
    except KeyError:
        print("no dataset")
        loadAllDatasets(datasetname, usemr)
    
    # load single dataset
    loadDataset(datasetname,dataset, usemr)
        

if __name__ == "__main__":
    main()

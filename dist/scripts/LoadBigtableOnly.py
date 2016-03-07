'''
Created on Mar 10, 2014

@author: neua
'''

import os
from string import Template
import subprocess
from sys import argv
import time 
from Util import replaceSpecialChars
from ImpalaWrapper import ImpalaWrapper


copy_bigtable_query = r"""

DROP TABLE IF EXISTS bigtable_parquet; 
CREATE TABLE bigtable_parquet LIKE bigtable_ext STORED AS PARQUETFILE;
set PARQUET_COMPRESSION_CODEC=snappy;
insert overwrite table bigtable_parquet select * FROM bigtable_ext;
 COMPUTE STATS bigtable_parquet;

"""

def main():
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
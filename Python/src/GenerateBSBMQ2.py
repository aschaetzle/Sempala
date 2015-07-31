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


query2 = Template(r"""
DROP TABLE IF EXISTS result;
 Create Table result as (SELECT  DISTINCT BGP1.propertyTextual4 AS "propertyTextual4" , BGP1.propertyTextual5 AS "propertyTextual5" , BGP1.propertyTextual2 AS "propertyTextual2" , BGP1.propertyTextual3 AS "propertyTextual3" , BGP1.propertyTextual1 AS "propertyTextual1" , BGP1.productFeature AS "productFeature" , BGP1.producer AS "producer" , BGP1.propertyNumeric4 AS "propertyNumeric4" , BGP1.label AS "label" , BGP1.propertyNumeric2 AS "propertyNumeric2" , BGP1.comme AS "comme" , BGP1.propertyNumeric1 AS "propertyNumeric1" 
 FROM 
(SELECT  DISTINCT BGP1_0.f AS "f" , BGP1_0.propertyTextual4 AS "propertyTextual4" , BGP1_0.propertyTextual5 AS "propertyTextual5" , BGP1_0.propertyTextual2 AS "propertyTextual2" , BGP1_0.propertyTextual3 AS "propertyTextual3" , BGP1_0.propertyTextual1 AS "propertyTextual1" , BGP1_2.productFeature AS "productFeature" , BGP1_1.p AS "p" , BGP1_1.producer AS "producer" , BGP1_0.label AS "label" , BGP1_0.propertyNumeric4 AS "propertyNumeric4" , BGP1_0.comme AS "comme" , BGP1_0.propertyNumeric2 AS "propertyNumeric2" , BGP1_0.propertyNumeric1 AS "propertyNumeric1" 
 FROM 
(SELECT  DISTINCT ID AS "p" , http___www_w3_org_2000_01_rdf_schema_label_ AS "producer" 
 FROM 
bigtable_parquet 
 WHERE 
ID is not null  AND http___www_w3_org_2000_01_rdf_schema_label_ is not null) BGP1_1 JOIN (SELECT  DISTINCT http___www4_wiwiss_fu_berlin_de_bizer_bsbm_v01_vocabulary_productPropertyTextual4_ AS "propertyTextual4" , http___www4_wiwiss_fu_berlin_de_bizer_bsbm_v01_vocabulary_productFeature_ AS "f" , http___www4_wiwiss_fu_berlin_de_bizer_bsbm_v01_vocabulary_productPropertyTextual5_ AS "propertyTextual5" , http___www4_wiwiss_fu_berlin_de_bizer_bsbm_v01_vocabulary_productPropertyTextual2_ AS "propertyTextual2" , http___www4_wiwiss_fu_berlin_de_bizer_bsbm_v01_vocabulary_productPropertyTextual3_ AS "propertyTextual3" , http___www4_wiwiss_fu_berlin_de_bizer_bsbm_v01_vocabulary_productPropertyTextual1_ AS "propertyTextual1" , http___purl_org_dc_elements_1_1_publisher_ AS "p" , http___www4_wiwiss_fu_berlin_de_bizer_bsbm_v01_vocabulary_productPropertyNumeric4_ AS "propertyNumeric4" , http___www_w3_org_2000_01_rdf_schema_label_ AS "label" , http___www4_wiwiss_fu_berlin_de_bizer_bsbm_v01_vocabulary_productPropertyNumeric2_ AS "propertyNumeric2" , http___www_w3_org_2000_01_rdf_schema_comment_ AS "comme" , http___www4_wiwiss_fu_berlin_de_bizer_bsbm_v01_vocabulary_productPropertyNumeric1_ AS "propertyNumeric1" 
 FROM 
bigtable_parquet 
 WHERE 
ID = '$prod' AND http___www_w3_org_2000_01_rdf_schema_label_ is not null AND http___www_w3_org_2000_01_rdf_schema_comment_ is not null AND http___www4_wiwiss_fu_berlin_de_bizer_bsbm_v01_vocabulary_producer_ is not null AND http___purl_org_dc_elements_1_1_publisher_ is not null AND http___www4_wiwiss_fu_berlin_de_bizer_bsbm_v01_vocabulary_productFeature_ is not null AND http___www4_wiwiss_fu_berlin_de_bizer_bsbm_v01_vocabulary_productPropertyTextual1_ is not null AND http___www4_wiwiss_fu_berlin_de_bizer_bsbm_v01_vocabulary_productPropertyTextual2_ is not null AND http___www4_wiwiss_fu_berlin_de_bizer_bsbm_v01_vocabulary_productPropertyTextual3_ is not null AND http___www4_wiwiss_fu_berlin_de_bizer_bsbm_v01_vocabulary_productPropertyNumeric1_ is not null AND http___www4_wiwiss_fu_berlin_de_bizer_bsbm_v01_vocabulary_productPropertyNumeric2_ is not null) BGP1_0 ON(BGP1_1.p=BGP1_0.p) JOIN (SELECT  DISTINCT ID AS "f" , http___www_w3_org_2000_01_rdf_schema_label_ AS "productFeature" 
 FROM 
bigtable_parquet 
 WHERE 
ID is not null  AND http___www_w3_org_2000_01_rdf_schema_label_ is not null) BGP1_2 ON(BGP1_0.f=BGP1_2.f)) BGP1) ; 
PROFILE; 
""")


query2hive = Template(r""" 
DROP TABLE IF EXISTS hiveresult;
 Create Table hiveresult as SELECT  DISTINCT BGP1.propertyTextual4 AS propertyTextual4 , BGP1.propertyTextual5 AS propertyTextual5 , BGP1.propertyTextual2 AS propertyTextual2 , BGP1.propertyTextual3 AS propertyTextual3 , BGP1.propertyTextual1 AS propertyTextual1 , BGP1.productFeature AS productFeature , BGP1.producer AS producer , BGP1.propertyNumeric4 AS propertyNumeric4 , BGP1.label AS label , BGP1.propertyNumeric2 AS propertyNumeric2 , BGP1.comme AS comme , BGP1.propertyNumeric1 AS propertyNumeric1 
 FROM 
(SELECT  DISTINCT BGP1_0.f AS f , BGP1_0.propertyTextual4 AS propertyTextual4 , BGP1_0.propertyTextual5 AS propertyTextual5 , BGP1_0.propertyTextual2 AS propertyTextual2 , BGP1_0.propertyTextual3 AS propertyTextual3 , BGP1_0.propertyTextual1 AS propertyTextual1 , BGP1_2.productFeature AS productFeature , BGP1_1.p AS p , BGP1_1.producer AS producer , BGP1_0.label AS label , BGP1_0.propertyNumeric4 AS propertyNumeric4 , BGP1_0.comme AS comme , BGP1_0.propertyNumeric2 AS propertyNumeric2 , BGP1_0.propertyNumeric1 AS propertyNumeric1 
 FROM 
(SELECT  DISTINCT ID AS p , http___www_w3_org_2000_01_rdf_schema_label_ AS producer 
 FROM 
bigtable_parquet 
 WHERE 
ID is not null  AND http___www_w3_org_2000_01_rdf_schema_label_ is not null) BGP1_1 JOIN (SELECT  DISTINCT http___www4_wiwiss_fu_berlin_de_bizer_bsbm_v01_vocabulary_productPropertyTextual4_ AS propertyTextual4 , http___www4_wiwiss_fu_berlin_de_bizer_bsbm_v01_vocabulary_productFeature_ AS f , http___www4_wiwiss_fu_berlin_de_bizer_bsbm_v01_vocabulary_productPropertyTextual5_ AS propertyTextual5 , http___www4_wiwiss_fu_berlin_de_bizer_bsbm_v01_vocabulary_productPropertyTextual2_ AS propertyTextual2 , http___www4_wiwiss_fu_berlin_de_bizer_bsbm_v01_vocabulary_productPropertyTextual3_ AS propertyTextual3 , http___www4_wiwiss_fu_berlin_de_bizer_bsbm_v01_vocabulary_productPropertyTextual1_ AS propertyTextual1 , http___purl_org_dc_elements_1_1_publisher_ AS p , http___www4_wiwiss_fu_berlin_de_bizer_bsbm_v01_vocabulary_productPropertyNumeric4_ AS propertyNumeric4 , http___www_w3_org_2000_01_rdf_schema_label_ AS label , http___www4_wiwiss_fu_berlin_de_bizer_bsbm_v01_vocabulary_productPropertyNumeric2_ AS propertyNumeric2 , http___www_w3_org_2000_01_rdf_schema_comment_ AS comme , http___www4_wiwiss_fu_berlin_de_bizer_bsbm_v01_vocabulary_productPropertyNumeric1_ AS propertyNumeric1 
 FROM 
bigtable_parquet 
 WHERE 
ID = '$prod' AND http___www_w3_org_2000_01_rdf_schema_label_ is not null AND http___www_w3_org_2000_01_rdf_schema_comment_ is not null AND http___www4_wiwiss_fu_berlin_de_bizer_bsbm_v01_vocabulary_producer_ is not null AND http___purl_org_dc_elements_1_1_publisher_ is not null AND http___www4_wiwiss_fu_berlin_de_bizer_bsbm_v01_vocabulary_productFeature_ is not null AND http___www4_wiwiss_fu_berlin_de_bizer_bsbm_v01_vocabulary_productPropertyTextual1_ is not null AND http___www4_wiwiss_fu_berlin_de_bizer_bsbm_v01_vocabulary_productPropertyTextual2_ is not null AND http___www4_wiwiss_fu_berlin_de_bizer_bsbm_v01_vocabulary_productPropertyTextual3_ is not null AND http___www4_wiwiss_fu_berlin_de_bizer_bsbm_v01_vocabulary_productPropertyNumeric1_ is not null AND http___www4_wiwiss_fu_berlin_de_bizer_bsbm_v01_vocabulary_productPropertyNumeric2_ is not null) BGP1_0 ON(BGP1_1.p=BGP1_0.p) JOIN (SELECT  DISTINCT ID AS f , http___www_w3_org_2000_01_rdf_schema_label_ AS productFeature 
 FROM 
bigtable_parquet 
 WHERE 
ID is not null  AND http___www_w3_org_2000_01_rdf_schema_label_ is not null) BGP1_2 ON(BGP1_0.f=BGP1_2.f)) BGP1; 
""")


def generateq2(listfile):
    with open(listfile) as f:
        products = f.readlines()
    index = 0
    for produ in products:
        result = query2.substitute(prod=str(produ).strip())
        with open('./corrected/q2_'+str(index)+'.sql', "w") as f:
            f.write(result)
        print(result)
        index = index +1 
    
    
    

def main():
    opts = getOpts(argv)
    try:
        list = opts['-f']
    except KeyError:
        print("no dataset")
        exit(404)
    
    # load single dataset
    generateq2(list)
        

if __name__ == "__main__":
    main()

Datasets must reside in /data/$DATASETNAME$/ on HDFS.

First, create a database for the dataset. In our experience,using Hue works best. The name of the database must be the name of your dataset file without any file extensions. 
E.g. /data/bsbm/berlin_2000k.nt will be stored in the database berlinn_2000k 


$DATASETNAME$ is either sp2bench, bsbm or lubm. E.g. /data/bsbm/berlin_2000k.nt

The Python scripts can be used to load the RDF datasets and query execution. Tables can be loaded, e.g. with the following command: 

python ./scripts/LoadBigtableImpala.py -omitparttable TRUE -d berlin_2000k -dn berlin

Options:
-d: dataset name, corresponds to filename on HDFS
-dn: Which type of benchmark data is loaded? E.g. 'lubm', 'berlin' or 'sp2bench' (This enable preprocessing for example if lubm is loaded.)
-omitparttable: Ignore partitioning tables by rdf:type property
-user: Specify HDFS user name. This determines the path of MapReduce output, /user/$HDFSUSER$/


If -d is omitted, all dataset sizes are loaded into the respective tables. Dataset names are hardcoded in the script and can be changed there.

Translate SPARQL:
java -cp SempalaTranslator.jar sparql2impala.run.Main -f ./queries/berlin_queries/queries3000k/ -i nofile -e
-f : Input folder
-i : Input file. Ignored if folder is used. 
-e : expand prefixes


Run queries:

python scripts/RunBerlinQueries.py -t 1 -f ./berlin_queries/queries2000k_v2 -d berlin_2000k
Options:
There exists a script for every benchmark.
-t: Time between each query in seconds. 
-f: Folder containing the SQL scripts. 
-d: Specific dataset. Name corresponds to the name of the dataset without file type extension. 
'-d' can be omitted


 

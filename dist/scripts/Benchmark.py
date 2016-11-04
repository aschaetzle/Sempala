'''
Created on Nov 1, 2013

@author: neua
'''

from sys import argv

from QueryIssue import issueQuery
from QueryIssueTripleStore import issueQueryTripleStore
from QueryIssueBigTable import issueQueryBigTable


def getOpts(argv):
    opts = {}
    while argv:
        if argv[0][0] == '-':
            opts[argv[0]] = argv[1]
            argv = argv[2:]
        else:
            argv = argv[1:]
    return opts

def benchmarkAllDatasets(form):
    datasets = ['dblp250K', 'dblp1M', 'dblp10M','dblp25M', 'dblp50M',  'dblp100M', 'dblp200M']
    for set in datasets:
        benchmarkDataset(set, form)

def benchmarkDataset(dataset, form):
    if(not form or form == 'vertical'):
        issueQuery(dataset, './results/vertical_part/'+dataset+'_text_nocomp.csv', "_text")
        issueQuery(dataset, './results/vertical_part/'+dataset+'_parquet_nocomp.csv', "")
        issueQuery(dataset, './results/vertical_part/'+dataset+'_parquet_gzip.csv', "_gzip")
        issueQuery(dataset, './results/vertical_part/'+dataset+'_parquet_snappy.csv', "_snappy")
#    if(not form or form == 'tripletable'):
#        issueQueryTripleStore(dataset+'_triplestore', 'triplestore_text', './results/triple_table/'+dataset+'_text_nocomp.csv')
#        issueQueryTripleStore(dataset+'_triplestore', 'triplestore_parquet', './results/triple_table/'+dataset+'_parquet_nocomp.csv')
#        issueQueryTripleStore(dataset+'_triplestore', 'triplestore_parquet_snappy', './results/triple_table/'+dataset+'_parquet_snappy.csv')
    if(not form or form == 'bigtable'):
#        issueQueryBigTable(dataset+'_bigtable', 'bigtable_text', './results/bigtable/'+dataset+'_text_nocomp.csv')
#        issueQueryBigTable(dataset+'_bigtable', 'bigtable_parquet', './results/bigtable/'+dataset+'_parquet_nocomp.csv')
#        issueQueryBigTable(dataset+'_bigtable', 'bigtable_parquet_gzip', './results/bigtable/'+dataset+'_parquet_gzip.csv')
        issueQueryBigTable(dataset+'_bigtable', 'bigtable_parquet_snappy', './results/bigtable/'+dataset+'_parquet_snappy.csv')

def main():
    opts = getOpts(argv)
    form = ""
    dataset = opts['-d']
    try: 
        form = opts['-f']
    except KeyError:
        print "no format"
    if not dataset:
        benchmarkAllDatasets(form)
    else: 
        benchmarkDataset(dataset, form)
   
if __name__ == '__main__':
    main()

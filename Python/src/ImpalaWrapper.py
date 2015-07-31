'''
Created on Nov 1, 2013

@author: neua
'''
from string import Template
import subprocess
import os
from os.path import join, dirname, basename

class ImpalaWrapper():
    impala_command = Template('impala-shell -i dbisma02.informatik.privat -d $d -o results.txt -B -q \'$q\'')    
    impala_command_file = Template('impala-shell -i dbisma02.informatik.privat -d $d -B -f $f ')    
    impala_command_stdout = Template('impala-shell -i dbisma02.informatik.privat -d $d -B -q \'$q\'')    

    results = []
    
    def __init__(self, database):
        proc = subprocess.Popen('impala-shell -i dbisma02.informatik.privat -B -q \'CREATE DATABASE IF NOT EXISTS '+database+'\'', stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        self.database = database
    
   

    def getRuntime(self):
        return self.runtime

    

    def getResults(self):
        return self.results
    
    def getNumResults(self):
        return self.numResults
        #proc = subprocess.Popen('wc -l results.txt', stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        #return proc.stdout.readlines()[0].split()[0]

    def queryToStdOut(self, query):
        print('Querying to standard output.')
        proc = subprocess.Popen(self.impala_command_stdout.substitute(q=query, d=self.database), stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = proc.communicate()
        print("Finished query")
        self.results = stdout.split('\n')
        for line in stderr.split('\n'):
            print(line)
            if line.find('Returned ') > -1 and line.find('in ') > -1:
                i = line.find('in')            
                self.runtime = line[i+3:].strip()
        return True

    def query(self, query):
        proc = subprocess.Popen(self.impala_command.substitute(q=query, d=self.database), stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        for line in proc.stderr.readlines():
            print(line)
            if line.find('Returned ') > -1 and line.find('in ') > -1:
                i = line.find('row')
                self.numResults = line[8:i].strip()
                i = line.find('in')            
                self.runtime = line[i+3:].strip()
        return True
    
    def queryFromFile(self, file):
        print("Querying file "+file)
        proc = subprocess.Popen(self.impala_command_file.substitute(f=file, d=self.database), stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = proc.communicate()
        print("Finished")
        self.numResults = ''
        self.runtime = ''
        for line in stderr.split('\n'):
            if line.find('Returned ') > -1 and line.find('in ') > -1:
                i = line.find('row')
                self.numResults = line[8:i].strip()
                i = line.find('in')
                if self.runtime == '':
                    self.runtime = line[i+3:].strip()
                else:             
                    self.runtime = self.runtime + " + " + line[i+3:].strip()
            else:
                print('Line did not contain any information: '+line)
        profFile = os.path.join(os.path.dirname(file), self.database, os.path.basename(file)+".prof")
        if not os.path.exists(os.path.dirname(profFile)):
            os.makedirs(os.path.dirname(profFile))
        f = open(profFile, 'w')
        f.write(stdout)
        f.close()
        return True

    def clearDatabase(self):
        query = "show tables;"
        self.queryToStdOut(query)
        for table in self.results:
            query ="CREATE TABLE IF NOT EXISTS "+table+" (ID string) ;"
            self.query(query)
            query ="Drop table "+table+" ;"
            self.query(query)
            query ="Drop table "+table+" ;"
            self.query(query)
    
    def computeStats(self):
        query = "show tables;"
        self.queryToStdOut(query)
        for table in self.results:
            query ="Compute stats "+table+" ;"
            self.query(query)

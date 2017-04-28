### Impala ExtVP
### Guide to execute Sempala in Extended Vertical Partitioning Multi Table Layout
To execute the loader of Sempala for ExtVP Multi Table Layout, the necessary parameters for the jar file that should be set are:
	 l presents the loader of Sempala,
	-d is the database where the ExtVP tables will be stored,
	-f is the format which in this case is extvp for ExtVP Multi Table Layout,
	-H is the host where Sempala loader is run,
	-i is the path to the folder on HDFS of input data for ExtVP Tables,
	-t is the threshold which is set for ExtVP Multi Table Layout,
	-ud is the path to the folder on HDFS of the user.
	
For example, the parameters of the jar could look like this:
java -jar Sempala.jar l -d extvpmultitabledatabase -f extvp -H dbisma03.informatik.privat -i /user/admin/WatDiv/100K -t 0.750 -ud /user/admin
		
Beside the necessary parameters, there are other parameters which can be added to the loader of Sempala for ExtVP Multi Table Layout, to execute it with different properties:
	-em to executes the loader without taking into consideration the threshold and storing all ExtVP tables,
	-lp is the path to the folder where the file with the list of predicates is located, to execute the loader only for those predicates in the list,
	-pp is the range of predicates from the list of all predicated for which the loader is executed.

For example, these optional parameters could look like this:
java -jar Sempala.jar l -d extvpmultitabledatabase -f extvp -H dbisma03.informatik.privat -i /user/admin/WatDiv/100K -t 0.75 -ud /user/admin -em -lp /home/ListofPredicated -pp 0,10

To execute the translator of Sempala for ExtVP Multi Table Layout, the necessary parameters for the jar file that should be set are:
	 t presents the translator of Sempala,
	-d is the database where the ExtVP tables are read from,
	-f is the format which in this case is extvp for ExtVP Multi Table Layout,
	-H is the host where Sempala translator is run,
	-i is the path to the folder of queries,
	-t is the threshold which is set for ExtVP Multi Table Layout.
	
For example, the parameters of the jar could look like this:
java -jar Sempala.jar t -d extvpmultitabledatabase -f extvp -H dbisma03.informatik.privat -i ./Q3 -t 0.5
		
Beside the necessary parameters, there are other parameters which can be added to the translator of Sempala for ExtVP Multi Table Layout, to execute it with different properties:
	-c to executes the querie of translator as "Count" queries and not as "Create Table As Select" queries,
	-rn is the format how the output table with the result should be named,
	-s to execute the queries with Straight Join with the order as the triple patterns are listerd in the query.
	
For example, these optional parameters could look like this:
java -jar Sempala.jar t -d extvpmultitabledatabase -f extvp -H dbisma03.informatik.privat -i ./Q3 -t 0.5 -c -rn ExtVPQ3result -s

### Complex Property Table Model

### Loader (Only working with Spark)
To execute the loader of Sempala Complex Property Table, is necessary to use the jar file as input of spark-submit command.
The necessary parameters for the jar file that should be set are:
	 l presents the loader of Sempala,
	-d is the database where the property table and the list of the properties will be stored,
	-f is the format which in this case is 'complex_property_table' for Complex Property Table model,
	-i is the path to the folder on HDFS of input data,
	
For example, the loader can be called like this:
spark-submit --class de.uni_freiburg.informatik.dbis.sempala.loader.run.Main Sempala.jar l -d ComplexWD100DB -f complex_property_table -i /user/admin/WatDiv/100K

### Translator Impala
To execute the translator of Sempala for Complex Property Table model using Impala, the necessary parameters for the jar file that should be set are:
	 t presents the translator of Sempala,
	-d is the database where the necessary tables are read from,
	-f is the format which in this case is 'complex_property_table',
	-H is the host where Sempala translator is run,
	-i is the path to the folder of queries,

For example, the parameters of the jar could look like this:
java -jar Sempala.jar t -d ComplexWD100DB -f complex_property_table -H dbisma03.informatik.privat -i ./Q3

### Translator Spark
To execute the translator of Sempala for Complex Property Table model using Spark, is necessary to use the jar file as input of spark-submit
The mandatory parameters for the jar file that should be set are:
	 t presents the translator of Sempala,
	-d is the database where the necessary tables are read from,
	-f is the format which in this case is 'complex_property_table_spark',
	-i is the path to the folder of queries,
For example, the translator can be called like this:
spark-submit --class de.uni_freiburg.informatik.dbis.sempala.translator.run.Main Sempala.jar t -d ComplexWD100DB -f complex_property_table_spark -i ./Q3

### optional properties
Beside the necessary parameters, there are other parameters which can be added to the Spark implementation of Sempala Complex Property Table translator, to execute it with different properties:
	-sp number of partitions in Spark. (see DataFrame#partitions)
For example, the translator can be called like this:
spark-submit --class de.uni_freiburg.informatik.dbis.sempala.translator.run.Main Sempala.jar t -d ComplexWD100DB -f complex_property_table_spark -i ./Q3 -sp 100

### Spark configurations
Moreover, some configurations of Spark framework can be added. We recommend to change them based on the environment where the jar is executed.
	--executor-memory amount of memory to use per executor process (e.g. 2g, 8g)
	--driver-memory amount of memory to use for the driver process, i.e. where SparkContext is initialized. (e.g. 1g, 2g)

For example, the translator with Spark configurations can be called like this:
spark-submit --executor-memory 16g --driver-memory 2g --class de.uni_freiburg.informatik.dbis.sempala.translator.run.Main Sempala.jar t -d ComplexWD100DB -f complex_property_table_spark -i ./Q3






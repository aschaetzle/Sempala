### THIS IS THE STATIC IN PROJECT REPOSITORY

Maven gets the impala-jdbc-driver from the static in-project repository. If
the current version in the static in-project repository is outdated install
the new one by downloading it from

http://www.cloudera.com/content/www/en-us/downloads.html.html

and installing it in the static in-project repository with the following
command (This will take care of checksum, system scope is deprecated):

mvn install:install-file
  -DlocalRepositoryPath=project_repo
  -DcreateChecksum=true
  -Dpackaging=jar
  -Dfile= <impala-jsbc.jar>
  -DgroupId=com.cloudera.impala
  -DartifactId=impala-jdbc-connector
  -Dversion= <version>



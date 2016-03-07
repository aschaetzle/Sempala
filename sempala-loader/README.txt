### WHAT IS THE PURPOSE OF THE "project_repo" DIRECTORY

Impala JDBC ships with several static libraries that are not versioned. Hence
there is no proper way to get them from a central repository as maven's or
cloudera's. For this reason we ship the libraries in a static in project
repository. The project repository contains only these dependecies that are
needed for the JDBC driver, but can not be retrieved by central repositories.
All other dependencies are resolved dynamically at build time by maven.

Each of them has been manually installed into the repository using the maven
install plugin. If the current version of impala in the static in-project repository
 should be updated, completely remove and rebuild  the project repository.

To build the project repo get the JDBC driver by downloading it from cloudera.com [1]. Public
libraries with few transitive dependencies shall be dynamically resolved by
maven. I.e. the pom.xml has to be adjusted. However there will be some libraries
that are not present in the central repositories. Install them in the project
repository with the install plugin. This will take care of checksum.

Example:

  mvn install:install-file
    -DlocalRepositoryPath=project_repo
    -DcreateChecksum=true
    -Dpackaging=jar
    -Dfile=<path_to:jdbc_driver.jar>
    -DgroupId=com.cloudera.impala
    -DartifactId=impala-jdbc-connector
    -Dversion=<version>

[1] http://www.cloudera.com/content/www/en-us/downloads.html.html

### INSTALLING A PACKAGE

Although rarely, but sometimes you will have 3rd party JARs that you need to put
in your local repository for use in your builds, since they don't exist in any
public repository like Maven Central. The JARs must be placed in the local
repository in the correct place in order for it to be correctly picked up by
Apache Maven. To make this easier, and less error prone, we have provide a goal
in the maven-install-plugin which should make this relatively painless. To
install a JAR in the local repository use the following command:

  mvn install:install-file -Dfile=<path-to-file> -DgroupId=<group-id> \
    -DartifactId=<artifact-id> -Dversion=<version> -Dpackaging=<packaging>

If there's a pom-file as well, you can install it with the following command:

  mvn install:install-file -Dfile=<path-to-file> -DpomFile=<path-to-pomfile>

With version 2.5 of the maven-install-plugin it gets even better. If the JAR was
built by Apache Maven, it'll contain a pom.xml in a subfolder of the META-INF
directory, which will be read by default. In that case, all you need to do is:

  mvn install:install-file -Dfile=<path-to-file>

(Source: https://maven.apache.org/guides/mini/guide-3rd-party-jars-local.html)




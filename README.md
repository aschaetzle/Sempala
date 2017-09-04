# Sempala

### LICENSE
Unless explicitly stated otherwise all files in this repository are licensed under the Apache Software License 2.0


### BUILD project
You need to have Maven installed on your system.
Then simply run "mvn package" from the root directory.
It will build 'sempala-loader', 'sempala-translator' and finally 'sempala'.
You can also build 'sepala-loader' or 'sempala-translator' only by
running "mvn package" from the corresponding subdirectory.
NOTE: Two jars are generated for sempala translator - one for Impala (sempala-translator) 
and one for Spark (spark-sempala-translator)


### PURPOSE OF project_repo DIRECTORY

Cloudera Impala JDBC connector ships with several libraries. All but the 
connector itself are available in the maven or cloudera central repositories
and are pulled at build time by maven. To fit in the maven architecture the
connector is installed in a in-project repository, which behaves like a remote 
central repository.

To update the version of the Impala JDBC connector in the in-project repository,
you can install a newer version of it and update the POM of sempala-parent
(main POM in root directory of this project) to use that version.
To do this, get the JDBC driver by downloading it from cloudera.com [1]
and install it with the maven install plugin. This will take care of checksums:

```
  mvn install:install-file
    -DlocalRepositoryPath=project_repo
    -DcreateChecksum=true
    -Dpackaging=jar
    -Dfile=<path_to:jdbc_driver.jar>
    -DgroupId=com.cloudera.impala
    -DartifactId=impala-jdbc-4.1-connector
    -Dversion=<version>
```


### Official guide to installing 3rd party JARs

Although rarely, but sometimes you will have 3rd party JARs that you need to put
in your local repository for use in your builds, since they don't exist in any
public repository like Maven Central. The JARs must be placed in the local
repository in the correct place in order for it to be correctly picked up by
Apache Maven. To make this easier, and less error prone, we have provide a goal
in the maven-install-plugin which should make this relatively painless. To
install a JAR in the local repository use the following command:

```
  mvn install:install-file -Dfile=<path-to-file> -DgroupId=<group-id> \
    -DartifactId=<artifact-id> -Dversion=<version> -Dpackaging=<packaging>
```

If there's a pom-file as well, you can install it with the following command:

```
  mvn install:install-file -Dfile=<path-to-file> -DpomFile=<path-to-pomfile>
```

With version 2.5 of the maven-install-plugin it gets even better. If the JAR was
built by Apache Maven, it'll contain a pom.xml in a subfolder of the META-INF
directory, which will be read by default. In that case, all you need to do is:

```
  mvn install:install-file -Dfile=<path-to-file>
```

(Source: https://maven.apache.org/guides/mini/guide-3rd-party-jars-local.html)



[1] http://www.cloudera.com/downloads/connectors/impala/jdbc.html

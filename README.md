# GSAT materialization with RDFox


## Installation

Install JRDFox 
```
mvn install:install-file -Dfile=<path to JRDFox.jar>
-DgroupId=tech.oxfordsemantic.jrdfox -DartifactId=jrdfox -Dversion=1.0 -Dpackaging=jar -DgeneratePom=true
```
Create a directory `RDFox-data` containing the license file.

Install watdiv and add the binary `watdiv` to the root of the project.

Compile the standalone jar
```
mvn clean compile assembly:single
```

# GSAT materialization with RDFox


## Usage


```
mvn exec:java -Dexec.mainClass="uk.ac.ox.cs.gsat.rdfox"
```

## Installation

Install JRDFox 
```
mvn install:install-file -Dfile=<path to JRDFox.jar>
-DgroupId=tech.oxfordsemantic.jrdfox -DartifactId=jrdfox -Dversion=1.0 -Dpackaging=jar -DgeneratePom=true
```

```
mvn clean compile assembly:single
```

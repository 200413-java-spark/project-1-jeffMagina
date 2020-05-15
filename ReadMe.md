# Students Performance Evaluation App
A server app which does Analysis on Student Performance in three categories: 
* Math
* Reading
* Writing 
based on Gender, Race, Parent Education, Provided Lunch and completion of a Test Preparation Course 
and displays selected transformed data to a web browser

## Build
### Java
>mvn clean compile 

## Usage
### For csv file batch load operation
>mvn exec:java -Dexec.args="loadCSV StudentsPerformance.csv"

### For running server
>mvn exec:java -Dexec.args="server"

## Design
### Architecture
- User uploads a CSV file
- Apache Spark use to read the file to an RDD
- RDD transformations performed on RDD to create analysis data
- Analysis data persisted to a PostgreSQL Docker instance on Amazon Web Services server
- Java Servlet in a Tomcat container displays Analysis to a Web Browser with simple HTML coding

### Main algorithm
- The main class parses args, and if they exist
    - If the first argument is "loadCSV [CSV File]"
        - CSV file will be parsed into a Apache Spark RDD
        - RDD will go through some transformations to get some analysis data
        - Analysis data RDDs will be inserted into a Linked Hash Map
        - The Analysis Linked Hash Map will be persisted to a postgreSQL Database running on a Docker Instance on a AWS instance
    - Else if the first argument is "server"
        - calls the startTomcat function and starts the tomcat server
        - tomcat container has a java servlet inside which outputs analysis data retrieved from postgreSQL Database to a web browser running on localhost

- Else a usage guide is printed to the console
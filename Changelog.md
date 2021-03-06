# Version 2.0.1
# Added
* J Unit testing for Spark Transformations 

# Changed
* pom.xml to include rounding dependencies and j unit dependencies
* Relocated MyCSVFile to Spark folder
* Moved LoadingCSVFile into SparkTransformations.java
* Rounded Average values to two decimals places
* Refactored ServerApp (main file) to be simpler, do less and be a bootstrap

# Version 2.0.0 
* --Presentable project
## Added
* data.html - simple html file to allow for user input using selector check boxes
* spark transformations for individual math score, individual reading score 
and individual writing score for each major test group and an overall count spark transformation
* A better way to display transformation data via java servlet instead of no named string data
using some html coding

## Changed
* Took out Debug printing from main ServerApp.java
* format of what data going to Database looks like

# Version 1.0.1
## Changed
* LoadCSVFile added caching for RDDs that are used over and over

# Version 1.0.0
## Added
* Schema for Database
* DockerFile to create Database on AWS server in a docker container
* read from database and insert into database in SQLRepo.java
* implemented read from database in JeffsProject1Servlet and Servlet displays database info
* LinkedHashMap to store RDD transformations in SparkTransformations.java
* Get method for LinkedHashMap in SparkTransformations
* Way in serverApp to just start server and not insert data via csv file
* Tomcat embeded server in ServerAPP (main)

## Changed
* Renamed HelloServlet.java to JeffsProject1Serlvet to better represent servlet name
* SQLDataSource to have correct URL for database
* pom.xml to incorporate tomcat embed
* LoadCSVFile removed debugging printouts
* SparkTransformations to have all private methods
* SparkTransformations constructor calls all private methods which do RDD transformations and stores them to a LinkedHashMap
* Sends RDD transformations to a database on an AWS server

## Deleted 
* removed servlet api from pom.xml

# Version 0.1.0
## Added
* connectToAWSServerViaSSH script
* Dao.java interface for data access objects like database retrieval
* SparkTransformation.java which does RDD transformation to StudentTest Data
* SqlDataSource which has Database connection info
* LoadCSVFile which loads CSVFile and maps it to a JavaRDD of type MyCSVFile
* SQLRepo.java which will eventually be able to insert and readall from database
* ServerApp which is essentially the main of the application which will:
** read and store CSV File as JavaRDD
** do spark manipulations
** save manipulations to postgresql database
** use Servlet to read and display manipulations from postgresql database

## Changed
* modified pom.xml, removed spark-sql as not needed, added postgresql dependency
* cleaned helloServlet to be ready to only read from database
* modified MyCSVFile to resemble the real dataset

## Deleted
* dummyData.cxsv
* deleted GoodbyeServlet not needed

# Version 0.0.1
## Added
* read in csv.file using apache spark
* servlet functionality
* lambda functions to do avging and counting of dummy data


# Version 0.0.1
## Added
* read in csv.file using apache spark
* servlet functionality
* lambda functions to do avging and counting of dummy data

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
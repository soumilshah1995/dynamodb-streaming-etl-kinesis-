# Building Transaction Apache Hudi Data Lake with Streaming ETL from Multiple Kinesis Streams and Joining using Apache Flink Part 1

### Video Tutorials 
* https://www.youtube.com/watch?v=hm2LKBCGTcU&feature=youtu.be

#### Step by Step written Guide 
* https://www.linkedin.com/pulse/building-transaction-apache-hudi-data-lake-streaming-etl-soumil-shah/?trackingId=vgnvZW7uTO2vpqBex8abPQ%3D%3D

#### Project Overview:
* We are attempting to execute ETL on streaming data in this project. You've used the Database per Service design pattern. Each service has an own database. However, some business transactions transcend numerous services, necessitating the need of a method to conduct such transactions. Assume you have two service orders and Customers. These services keep their own local databases, but businesses want to access stitched data for some insights, thus we'll use the SAGA Pattern, in which each microservice broadcasts events on its own streams. We will cleanse the data, join it with Apache Flink, and insert  the curated data into the next stream where the glue streaming task may take the curated data and execute UPSERT on Apache Hudi data lakes. This way we are performing streaming ETL and data is available for use within < 5 minutes

####  Architecture:
![stream drawio (1)](https://user-images.githubusercontent.com/39345855/210186029-bcd75f75-aedd-4fbc-a46c-13baaec40e18.png)

####  Explanation:
* Database-per-service pattern is a software architecture pattern that creates a separate database for each service instead of having one single database. This approach is usually used in microservices architectures to ensure that each service is decoupled from the other and that each service is free to choose its own database technology and schema. This approach also allows for easier scaling and maintenance of the system as each service can be independently managed and updated. To bring data from this system you can use either debezium or AWS DMS or leverage streams option if available. In the given scenarios both the team who are operating this microservices have opted for dynamodb as their preferred choice of database.

* We shall leverage DynamoDB Streams and captures changes happening into database and capture these changes and process them by lambda functions and one preprocessing is done we can now insert the data into its respective streams. We shall leverage the use of Apache Flink to stich (JOIN) the data and output the curated data into next streams. 
* Glue streaming job can now consume the messages from curated streams and then perform UPSERT into hudi tables. This is where we will have curated data where there would be no duplicates and hence this would be out transaction datalake. User can run ad hoc query and build BI dashboards uysing Quicksights and create views in athena if needed.

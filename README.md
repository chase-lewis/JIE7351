# Documentation

## Terminology and Acronyms
 - **CDC (Centers for Disease Control and Prevention)**: The CDC is one of the primary operating components of the United States Department of Health and Human Services.
 - **ETL (Extract, Transform, Load)**: The process of extracting information from one database, transforming it into a new format, and loading it into another database.
 - **Health Partners**: The Centers for Disease Control and Prevention works with other health agencies and organizations in its effort to strengthen public health. These can include other government agencies, academic institutions, foundations, non-governmental institutions, faith-based organizations, and private entities.
 - **JDBC (Java Database Connectivity)**: Industry standard for database-independent connectivity between the Java programming language and a wide range of databases.
 - **Kafka**: Apache Kafka is an open-source distributed streaming platform that is used for building real-time data pipelines and streaming applications.
 - **Kafka Cluster**: Kafka entity that stores streams of records in categories called topics. Kafka is run as a cluster on one or more servers.
 - **Kafka Producer**: Kafka entity that allows applications to send streams of data to topics in the Kafka cluster.
 - **Kafka Consumer**: Kafka entity that allows applications to read streams of data from topics in the Kafka Cluster.
 - **KSQL**: Confluent component that allows one to read, write, and process streaming data in real-time using SQL-like semantics.
 - **NEDSS (National Electronic Disease Surveillance System)**: NEDSS allows states and health partners to enter disease data into a database accessible to health investigators. It allows labs to upload reports to health partners, integrates multiple databases into a single integrated system, and allows partners to send and share information. The foundational data component of the NEDSS is called the NEDSS Base System (NBS).
 - **ODS (Operational Datastore)**: Database designed for transaction management amongst health partners and related health officials. Source for the ETL process.
 - **SAS**: Software currently used for data storage, manipulation and analysis. Proprietary service we are tasked with replacing.
 - **Schema Registry**: A component of Confluent Open Source that stores a versioned history of all schemas and allows for the changing of schemas based on previous versions.
 - **RDB (Reporting Database)**: Database designed for data analysis and reporting. Sink for the ETL process.

## Project Introduction
The Centers for Disease Control (CDC) and Prevention maintains an integrated information system called the National Electronic Disease Surveillance System (NEDSS). The NEDSS Base System allows states and health partners to enter disease data into a database that is accessible to health investigators. It allows labs to upload reports to health partners, integrates multiple databases into a single integrated system, and allows partners to send and share information. This system is used in twenty-three states nationwide.

Currently, there is an Extract, Transform, and Load process implemented in the system that converts health partner data into a format that more closely resembles traditional public health databases. The current ETL process is implemented via SAS, a proprietary statistical analysis software for which the CDC must pay licensing fees within each state. Our task is to aid the CDC in researching, designing, and implementing a new ETL process using open source software that can be easily built upon and used for all data dimensions of the NBS.

By writing an ETL process for the CDC that can be used without payment or third party restrictions, we will free up both monetary resources and contractionary restrictions for the organization. Our research will entail looking for an open source product that can be used instead of writing our own ETL tool specific to the CDC. Although the scope of our project is initially limited to only one area of the Operational Datastore (ODS), our solution must be generalizable so that it can be implemented system-wide in the future. Our research and development process is guided by important requirements including implementation simplicity, user adaptability, and computational efficiency.

## Apache Kafka

### Overview
Kafka is a publish/subscribe messaging system designed to solve this problem. It is often described as a “distributed commit log” or more recently as a “distributing streaming platform.” A filesystem or database commit log is designed to provide a durable record of all transactions so that they can be replayed to consistently build the state of a system. Similarly, data within Kafka is stored durably, in order, and can be read deterministically. In addition, the data can be distributed within the system to provide additional protections against failures, as well as significant opportunities for scaling performance.

Kafka clients are users of the system, and there are two basic types: producers and consumers. There are also advanced client APIs—Kafka Connect API for data integration and Kafka Streams for stream processing. The advanced clients use producers and consumers as building blocks and provide higher-level functionality on top.

Producers create new messages. In other publish/subscribe systems, these may be called publishers or writers. In general, a message will be produced to a specific topic. By default, the producer does not care what partition a specific message is written to and will balance messages over all partitions of a topic evenly. In some cases, the producer will direct messages to specific partitions. This is typically done using the message key and a partitioner that will generate a hash of the key and map it to a specific partition. This assures that all messages produced with a given key will get written to the same partition. The producer could also use a custom partitioner that follows other business rules for mapping messages to partitions.

Consumers read messages. In other publish/subscribe systems, these clients may be called subscribers or readers. The consumer subscribes to one or more topics and reads the messages in the order in which they were produced. The consumer keeps track of which messages it has already consumed by keeping track of the offset of messages. The offset is another bit of metadata—an integer value that continually increases—that Kafka adds to each message as it is produced. Each message in a given partition has a unique offset. By storing the offset of the last consumed message for each partition, either in Zookeeper or in Kafka itself, a consumer can stop and restart without losing its place. Consumers work as part of a consumer group, which is one or more consumers that work together to consume a topic. The group assures that each partition is only consumed by one member. The mapping of a consumer to a partition is often called ownership of the partition by the consumer. In this way, consumers can horizontally scale to consume topics with a large number of messages. Additionally, if a single consumer fails, the remaining members of the group will rebalance the partitions being consumed to take over for the missing member.

### Confluent

Confluent serves as a bundler for all components required to run Kafka. Installation provides Zookeeper, which is used for storing metadata and coordinating clusters, Kafka itself, and a Schema Registry for managing the types of data being written to Kafka. It also provides bash scripts for running, managing, and shutting down each component. This repository includes versions of these scripts translated into Batch for Windows support.

### KSQL
KSQL is a streaming SQL engine that enables stream processing against Apache Kafka. By ‘Streaming SQL Engine’ we mean that it can allow one to query, read, write, and process data in real-time and at scale using intuitive SQL-like syntax. It supports windowing, sessionization, joins, and aggregations. KSQL continuously queries on topics, hence it features a real time application for data analytics. It has several use cases including anomaly detection, data monitoring, and of course streaming ETL applications. KSQL makes it simple to transform data within a Kafka pipeline, readying messages to cleanly land in another system.

To briefly touch on a few core concepts we will first discuss KSQL’s two primary components. Firstly, there is KSQL’s command line interface which is an interactive interface for writing KSQL queries on the client side. There is also, of course, the KSQL server which is the engine actually executing the KSQL queries. It includes data processing, as well as reading data from and writing data to the target Kafka Cluster.

There are two key terms when discussing KSQL. A KSQL Stream is an unbounded sequence of structured data or ‘facts.’ Facts in a stream are completely immutable, which means new facts can be inserted to a stream, but existing facts can never be updated or deleted. Streams can be created from a Kafka topic or derived from an existing stream. A stream’s underlying data is durable stored within a Kafka topic on the Kafka brokers. A KSQL Table is a view of a stream, or another table, and represents a collection of evolving facts. It is the equivalent of a traditional database table but enriched by steaming semantics such as windowing. Facts are mutable (as compared to the streams immutable facts) which means new facts can be inserted to the table, and existing facts can be updated or deleted. Tables can be created from a Kafka topic or derived from existing streams and tables. Tables can be created from a Kafka topic or derived from existing streams and tables. In both cases, a table’s underlying data is durable stored within a Kafka topic on the Kafka brokers.

### Hardware Requirements

#### Disk Capacity

The amount of disk capacity that is needed is determined by how many messages need to be retained at any time. If the broker is expected to receive 1 TB of traffic each day, with 7 days of retention, then the broker will need a minimum of 7 TB of usable storage for log segments. You should also factor in at least 10% overhead for other files, in addition to any buffer that you wish to maintain for fluctuations in traffic or growth over time. Storage capacity is one of the factors to consider when sizing a Kafka cluster and determining when to expand it. The total traffic for a cluster can be balanced across it by having multiple partitions per topic, which will allow additional brokers to augment the available capacity if the density on a single broker will not suffice.

#### Memory

The normal mode of operation for a Kafka consumer is reading from the end of the partitions, where the consumer is caught up and lagging behind the producers very little, if at all. In this situation, the messages the consumer is reading are optimally stored in the system’s page cache, resulting in faster reads than if the broker has to reread the messages from disk. Therefore, having more memory available to the system for page cache will improve the performance of consumer clients. Kafka itself does not need much heap memory configured for the Java Virtual Machine. Even a broker that is handling X messages per second and a data rate of X megabits per second can run with a 5 GB heap. The rest of the system memory will be used by the page cache and will benefit Kafka by allowing the system to cache log segments in use. This is the main reason it is not recommended to have Kafka collocated on a system with any other significant application, as they will have to share the use of the page cache. This will decrease the consumer performance for Kafka.

#### CPU

Processing power is not as important as disk and memory, but it will affect overall performance of the broker to some extent. Ideally, clients should compress messages to optimize network and disk usage. The Kafka broker must decompress all message batches, however, in order to validate the checksum of the individual messages and assign offsets. It then needs to recompress the message batch in order to store it on disk. This is where the majority of Kafka’s requirement for processing power comes from.

#### AWS

AWS provides many compute instances, each with a different combination of CPU, memory, and disk, and so the various performance characteristics of Kafka must be prioritized in order to select the correct instance configuration to use. A good place to start is with the amount of data retention required, followed by the performance needed from the producers. If very low latency is necessary, I/O optimized instances that have local SSD storage might be required. Otherwise, ephemeral storage (such as the AWS Elastic Block Store) might be sufficient. Once these decisions are made, the CPU and memory options available will be appropriate for the performance. In real terms, this will mean that for AWS either the m4 or r3 instance types are a common choice. The m4 instance will allow for greater retention periods, but the throughput to the disk will be less because it is on elastic block storage. The r3 instance will have much better throughput with local SSD drives, but those drives will limit the amount of data that can be retained. For the best of both worlds, it is necessary to move up to either the i2 or d2 instance types, which are significantly more expensive.

## Release Notes - Version 0.1

### Features

 - Kafka cluster configuration with support for the Windows environment
 - Extendible Connectors for reading and writing from SQL Server databases
 - Support for running KSQL on Windows
 - Producer for streaming data from the ODS
 - Consumer for transforming data from Kafka and writing to the RDB
 - Proof of concept Consumer for extracting, transforming, and loading racial information in real time


### Issues

 - KSQL is still in early development and cannot currently be used.
 - Not all ODS data is transferred through Kafka, requiring the Consumer to make additional queries to the ODS. This is less space and time efficient than an ideal KSQL solution.

## Installation Instructions

### Prerequisites and Dependencies

 - [Java 8 jdk](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html target="_blank")
 - <a href="http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html" target="_blank">Java 8 jdk</a>
 - [Microsoft SQL Server Management Studio connection setup](https://www.top-password.com/blog/how-to-enable-remote-connections-in-sql-server/ target="_blank")
 - [Enabling the right ports for remote connection](https://blogs.msdn.microsoft.com/walzenbach/2010/04/14/how-to-enable-remote-connections-in-sql-server-2008/ target="_blank")

### Setup

 - Begin by cloning the following git repo to an appropriate location on your system:

<https://github.com/chase-lewis/JIE7351.git>

 - Edit the Database.properties file in ```etc/cdc_properties```

     - Replace the ip and user/password for the connection.url with the correct ip and credentials of the database

```connection.url=jdbc:sqlserver://128.61.27.82:1433;DatabaseName=NBS_ODSE;user=test;password=test;```

 - Edit the Connector.java file in ```src/Connector.java```

     - Replace the user/password for the connectionUrl with the correct credentials of the database

```private String connectionUrl[] = {"jdbc:sqlserver://", ":1433;user=test;password=test;"};```

 - Edit the KafkaConsumerTest.java in ```src/KafkaConsumerTest.java```

     - Replace the second argument of ```config.put("bootstrap.servers", "localhost:9092");``` line 173 with the correct ip and port of your kafka cluster, if you are hosting the kafka cluster on a different machine than the machine running the consumer and/or producer. Default port is 9092, but can be configured.

    - Replace the third argument of the constructor with the IP of your Database

```KafkaConsumerTest test = new KafkaConsumerTest(config, alist, "128.61.27.82");```

### Running

#### Windows

 - Start Zookeeper, Kafka, and the Schema Registry with the following command from the parent level directory:

`bin/windows/start-all.bat`

 - To start the producer:

`src/standalone_producer.bat`

 - To start the consumer:

`src/make_consumer.bat`

 - To start the ksql-cli:

`ksql/bin/windows/ksql-run-class.bat`

#### Unix

 - Start Zookeeper, Kafka, and the Schema Registry with the following command from the parent level directory:

`bin/confluent start`

 - To start the producer:

`src/standalone_producer.sh`

 - To start the consumer:

`src/make_consumer.sh`

 - To start the ksql-cli:

`$ LOG_DIR=./ksql_logs <path-to-confluent>/bin/ksql`

## Further development

The files we changed in order to get kafka to run on windows:

### Batch files to start kafka, schema-registry and zookeeper, identical to confluent start command for unix
 - For starting all components at once:

`bin/windows/start-all.bat`

 - For starting the componenets individually (must be run in this order):

`bin/windows/start-zookeeper.bat`

`bin/windows/start-kafka.bat`

`bin/windows/start-schema.bat`


### The properties files used by the standalone producer built into kafka
The folder `etc/cdc_properties` and all the properties inside it
`etc/cdc_properties/Database.properties`

#### The source files we created
 - The connector for connecting to the ms sql server:

`src/Connector.java`

 - The customized consumer with transformation logic and configuration for writing to the RDB:

`src/KafkaConsumerTest.java`

 - The barebones producer, not currently used:

`src/KafkaProducerTest.java`

 - The batch file that runs KafkaConsumerTest and includes all the required jar files to run the java:

`src/make_consumer.bat`

 - The batch file that runs KafkaProducerTest and includes all the required jar files:

`src/make_producer.bat`

 - The batch file similar to the standalone_producer script in `/bin`. It runs kafka’s standalone producer, passing in the Database.properties file as the properties:

`src/standalone_producer.bat`

 - The batch file to start ksql:

`ksql/bin/windows/ksql-run-class.bat`

### Troubleshooting

 - Kafka will not start: Verify that the system has a functioning installation of the Java 8 jdk.

     - If any batch file closes without error, try running it through the command prompt instead of double clicking.

     - If `bin/windows/start-all.bat` has any batch files close without error, try running `start-kafka.bat` `start-schema.bat` and `start-zookeeper.bat` individually through command prompt instead of double clicking.

     - Delete system `/tmp/zookeeper` and `/tmp/kafka-logs` files

 - Cannot connect to ODS or RDB: Ensure the correct credentials and ip are changed in the Database.properties, Connector.java and KafkaConsumerTest.java. Refer to the setup section for more details.

 - Compile errors: Check the compile strings in make_*.bat are pointing to the correct locations of the jar files.


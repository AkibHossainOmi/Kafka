# Kafka Producer and Consumer in Java

Download Apache Kafka from Binary downloads for example,

<a href="https://downloads.apache.org/kafka/3.5.1/kafka_2.12-3.5.1.tgz">Scala 2.12  - kafka_2.12-3.5.1.tgz</a>

Extract the `kafka_2.12-3.5.0.tgz` file and rename it as `Kafka`.

Copy the directory of `Kafka` and open `zookeeper.properties` from the directory,
```
./kafka/config
```
From `zookeeper.properties` edit the line `dataDir=/tmp/zookeeper` to set the `Kafka` directory such as `dataDir=kafkadirectory`. 
<br>
For example,
If the `Kafka` directory is,
```
/home/omi/kafka
``` 
Then the line will be,
```
dataDir=/home/omi/kafka
```
Now, open terminal from the `Kafka` directory and run,
```
bin/zookeeper-server-start.sh config/zookeeper.properties
```

Open new tab or window of the terminal from the same directory and run,
```
bin/kafka-server-start.sh config/server.properties
```

Install `IntelliJ IDEA` and `openjdk version "1.8.0_362"`
<br>
Now, you can clone the repository and run the Java producer and consumer to create topic, produce message and consume those message.





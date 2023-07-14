package com.example.kafka;

//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerConfig;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.common.serialization.StringSerializer;
//
//import java.util.Properties;
//
//public class Producer {
//    public static void main(String[] args) {
//        System.out.println("I am a Kafka Producer");
//
//        String bootstrapServers = "127.0.0.1:9092";
//
//        Properties properties = new Properties();
//        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//
//        // create the producer
//        final KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
//
//        // create a producer record
//        ProducerRecord<String, String> producerRecord =
//                new ProducerRecord<>("telco_bl1", "hi I am Humayun Ahmed");
//
//        // send data - asynchronous
//        producer.send(producerRecord);
//
//        // flush data - synchronous
//        producer.flush();
//
//        // flush and close producer
//        producer.close();
//
//    }
//}
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.producer.*;

import java.util.Collections;
import java.util.Properties;
import java.util.Scanner;

public class KafkaProducerExample {
    public static void main(String[] args) {
        // Configure the producer
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092"); // Kafka broker(s) address
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Create the Kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Create a Kafka record with a topic and a message
        String topic = null;

        // Set properties for the AdminClient
        Properties props1 = new Properties();
        props1.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Create an AdminClient
        try (AdminClient adminClient = AdminClient.create(props1)) {
            // Get a list of existing topics
            ListTopicsResult topicsResult = adminClient.listTopics();
            java.util.Set<String> topicNames = topicsResult.names().get();
            System.out.println("Enter topic-name: ");
            Scanner scanner = new Scanner(System.in);
            topic=scanner.nextLine();
            // Check if the desired topic exists
            if (topicNames.contains(topic)) {
                System.out.println("Topic '" + topic + "' exists.");
            } else {


                // Create a new topic
                int numPartitions = 3;
                short replicationFactor = 1;

                NewTopic newTopic = new NewTopic(topic, numPartitions, replicationFactor);
                adminClient.createTopics(Collections.singleton(newTopic)).all().get();
                System.out.println("Topic '" + topic + "' created.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Read input from the user
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter messages (press Ctrl+C to exit):");
        while (true) {
            String message = scanner.nextLine();

            // Send the message to Kafka
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        exception.printStackTrace();
                    } else {
                        System.out.println("Message sent successfully! Offset: " + metadata.offset());
                    }
                }
            });
        }
    }
}

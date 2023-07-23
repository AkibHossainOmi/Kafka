package com.example.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Collections;
import java.util.Properties;
import java.util.Scanner;

public class Producer
{

    public static Properties properties;
    public static KafkaProducer<String, String> producer;

    static void InitialiseProducer(String TopicName, int PartitionSize, String BootstrapAddress, String Port)
    {
        String newBrokerAddress = BootstrapAddress + ":" + Port;
        properties = new Properties();
        properties.put("bootstrap.servers", BootstrapAddress); // Kafka broker(s) address
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, newBrokerAddress);
        // Create the Kafka producer
        producer = new KafkaProducer<>(properties);
        // Set properties for the AdminClient
        Properties props1 = new Properties();
        props1.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, newBrokerAddress);

        // Create an AdminClient
        try (AdminClient adminClient = AdminClient.create(props1))
        {
            // Get a list of existing topics
            ListTopicsResult topicsResult = adminClient.listTopics();
            java.util.Set<String> topicNames = topicsResult.names().get();
            // Check if the desired topic exists
            if (topicNames.contains(TopicName))
            {
                System.out.println("Topic '" + TopicName + "' exists.");
            }
            else
            {
                short replicationFactor = 1;
                NewTopic newTopic = new NewTopic(TopicName, PartitionSize, replicationFactor);
                adminClient.createTopics(Collections.singleton(newTopic)).all().get();
                System.out.println("Topic '" + TopicName + "' created.");
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    static void SendMessage(String TopicName)
    {
        // Read input from the user
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter messages (press Ctrl+C to exit):");
        while (true) {
            System.out.println("Enter JSON string or type 'exit' to quit:");
            String userInput = scanner.nextLine();

            if ("exit".equalsIgnoreCase(userInput.trim())) {
                break; // Exit the loop if user types 'exit'
            }

            try {
                // Send JSON message to Kafka topic
                String jsonString = userInput;
                ProducerRecord<String, String> record = new ProducerRecord<>(TopicName, 0, "0", jsonString);
                int i=1;
                while(i>0)
                {
                    producer.send(record);
                    System.out.println("Message sent to Kafka topic successfully!");
                }

            } catch (Exception e) {
                System.out.println("Invalid JSON string. Please try again.");
            }
        }
    }

    public static void main(String[] args)
    {
        InitialiseProducer("telco_gp2",3,"localhost","9092");
        SendMessage("telco_gp2");
    }
}
//{ "data": [{ "smsId": "000004", "callingPartyNumber": "8809678123200", "tailInstances": null, "message": "Hi", "encoding": "1", "parentId": null, "destinationNumber": "8801743801850" }, { "smsId": "689596", "callingPartyNumber": "8812345680", "tailInstances": null, "message": "Hellooooooooooooooooooo", "encoding": "1", "parentId": null, "destinationNumber": "8801222223850" }]}
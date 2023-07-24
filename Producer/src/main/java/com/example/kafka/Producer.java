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

public class Producer
{

    public static Properties properties;
    public static KafkaProducer<String, String> producer;

    static void createProducer(@SuppressWarnings("SameParameterValue") String TopicName, @SuppressWarnings("SameParameterValue") int numberOfPartition, @SuppressWarnings("SameParameterValue") String BootstrapAddress, @SuppressWarnings("SameParameterValue") int Port)
    {
        String newBrokerAddress = BootstrapAddress + ":" + Port;
        properties = new Properties();
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, newBrokerAddress);

        producer = new KafkaProducer<>(properties);

        Properties propertiesAdminClient = new Properties();
        propertiesAdminClient.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, newBrokerAddress);


        try (AdminClient adminClient = AdminClient.create(propertiesAdminClient))
        {
            ListTopicsResult topicsResult = adminClient.listTopics();
            java.util.Set<String> topicNames = topicsResult.names().get();

            if (topicNames.contains(TopicName))
            {
                System.out.println("Topic '" + TopicName + "' exists.");
            }
            else
            {
                short replicationFactor = 1;
                NewTopic newTopic = new NewTopic(TopicName, numberOfPartition, replicationFactor);
                adminClient.createTopics(Collections.singleton(newTopic)).all().get();
                System.out.println("Topic '" + TopicName + "' created.");
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    static void dumpMessage(@SuppressWarnings("SameParameterValue") String TopicName, @SuppressWarnings("SameParameterValue") String Message)
    {
        ProducerRecord<String, String> record = new ProducerRecord<>(TopicName, 0, "0", Message);
        producer.send(record);
        System.out.println("Message sent to Kafka topic successfully!");
    }

    public static void main(String[] args)
    {
        createProducer("telco_gp2",3,"localhost",9092);
        dumpMessage("telco_gp2", "{ \"data\": [{ \"smsId\": \"000004\", \"callingPartyNumber\": \"8809678123200\", \"tailInstances\": null, \"message\": \"Hi\", \"encoding\": \"1\", \"parentId\": null, \"destinationNumber\": \"8801743801850\" }]}");
    }
}
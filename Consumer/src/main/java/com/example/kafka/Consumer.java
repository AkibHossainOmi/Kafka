package com.example.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer
{
    public static String NewBrokerAddress;
    public static Properties properties = new Properties();
    public static org.apache.kafka.clients.consumer.Consumer<String, String> consumer;
    static void InitialiseConsumer(String BootstrapAddress, String Port)
    {
        NewBrokerAddress=BootstrapAddress + ":" + Port;

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, NewBrokerAddress);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "#telco2023");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new KafkaConsumer<>(properties);
    }

    static void ReadMessage(String TopicName, int Partition) throws JsonProcessingException, InterruptedException {
        // Assign the consumer to a specific partition
        TopicPartition topicPartition = new TopicPartition(TopicName, Partition);
        consumer.assign(Collections.singletonList(topicPartition));

        // Seek to the beginning of the partition
        //consumer.seekToBeginning(Collections.singletonList(topicPartition));



        // Poll for new messages
        while (true)
        {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            String smsId, callingPartyNumber, message, destinationNumber;
            for (ConsumerRecord<String, String> record : records) {
                //String key = record.key();
                String value = record.value();
                System.out.println("Offset = " + record.offset());
                System.out.println("");

                ObjectMapper objectMapper = new ObjectMapper();
                JsonNode jsonNode = objectMapper.readTree(value);
                int i=0;
                // Read values from the JSON using key names
                JsonNode dataNode = jsonNode.get("data");
                if (dataNode.isArray() && dataNode.size() > 0)
                {
                    i=0;
                    while(i<dataNode.size())
                    {
                        JsonNode firstDataItem = dataNode.get(i);
                        if(firstDataItem.get("tailInstances").asText()!=null)
                        {
                            JsonNode tailinstancesNode = firstDataItem.get("tailInstances");

                            int j=0;
                            while(j<tailinstancesNode.size())
                            {
                                JsonNode secondDataItem = tailinstancesNode.get(j);
                                smsId = secondDataItem.get("smsId").asText();
                                callingPartyNumber = secondDataItem.get("callingPartyNumber").asText();
                                message = secondDataItem.get("message").asText();
                                destinationNumber = secondDataItem.get("destinationNumber").asText();

                                // Print the values
                                System.out.println("SMS ID: " + smsId);
                                System.out.println("Calling Party Number: " + callingPartyNumber);
                                System.out.println("Message: " + message);
                                System.out.println("Destination Number: " + destinationNumber);
                                System.out.println("Destination Number: " + destinationNumber);
                                System.out.println("");
                                j++;
                            }
                        }
                        smsId = firstDataItem.get("smsId").asText();
                        callingPartyNumber = firstDataItem.get("callingPartyNumber").asText();
                        message = firstDataItem.get("message").asText();
                        destinationNumber = firstDataItem.get("destinationNumber").asText();

                        // Print the values
                        System.out.println("SMS ID: " + smsId);
                        System.out.println("Calling Party Number: " + callingPartyNumber);
                        System.out.println("Message: " + message);
                        System.out.println("Destination Number: " + destinationNumber);
                        System.out.println("Destination Number: " + destinationNumber);
                        System.out.println("");
                        i++;
                    }
                }
                else
                {
                    System.out.println("No data found or data is not an array.");
                }
                // Manually commit the offset after processing the message
                consumer.commitSync(Collections.singletonMap(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1)
                ));
                //Thread.sleep(1000);
            }
        }

    }

    public static void main(String[] args) throws JsonProcessingException, InterruptedException {
        InitialiseConsumer("localhost", "9092");
        ReadMessage("telco_gp2", 0);
    }
}
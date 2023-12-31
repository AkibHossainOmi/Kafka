package com.example.kafka;

import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.List;

class SmsEntry {
    private String smsId;
    private String callingPartyNumber;
    private List<TailInstance> tailInstances;
    private String message;
    private String encoding;
    private String parentId;
    private String destinationNumber;

    public String getSmsId() {
        return smsId;
    }

    public String getCallingPartyNumber() {
        return callingPartyNumber;
    }

    public List<TailInstance> getTailInstances() {
        return tailInstances;
    }

    public String getMessage() {
        return message;
    }

    public String getEncoding() {
        return encoding;
    }


    public String getParentId() {
        return parentId;
    }

    public String getDestinationNumber() {
        return destinationNumber;
    }

}

class TailInstance {
    private String smsId;
    private String callingPartyNumber;
    private List<TailInstance> tailInstances;
    private String message;
    private String encoding;
    private String parentId;
    private String destinationNumber;

    // Add getters and setters here

    public String getSmsId() {
        return smsId;
    }


    public String getCallingPartyNumber() {
        return callingPartyNumber;
    }


    public List<TailInstance> getTailInstances() {
        return tailInstances;
    }


    public String getMessage() {
        return message;
    }


    public String getEncoding() {
        return encoding;
    }


    public String getParentId() {
        return parentId;
    }


    public String getDestinationNumber() {
        return destinationNumber;
    }

}

public class Consumer
{
    public static String NewBrokerAddress;
    public static Properties properties = new Properties();
    public static org.apache.kafka.clients.consumer.Consumer<String, String> consumer;
    static void InitialiseConsumer(String BootstrapAddress,String Port)
    {
        NewBrokerAddress=BootstrapAddress + ":" + Port;

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, NewBrokerAddress);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,1);

        consumer = new KafkaConsumer<>(properties);
    }

    static void ReadMessage(String topicName, int noOfPartition) throws InterruptedException {

        TopicPartition topicPartition = new TopicPartition(topicName, noOfPartition);
        consumer.assign(Collections.singletonList(topicPartition));

        // Seek to the beginning of the partition
        // consumer.seekToBeginning(Collections.singletonList(topicPartition));

        new Thread(() ->
        {
            //noinspection InfiniteLoopStatement
            while (true) {
                // Poll for new messages
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.timestamp());
                    long offset = record.offset();
                    String value = record.value();
                    System.out.println("Offset: " + offset + "\nValue: " + value);

                    Gson gson = new Gson();

                    // Convert JSON to Java object
                    SmsData smsData = gson.fromJson(value, SmsData.class);

                    // Now you can access the data in the smsData object
                    List<SmsEntry> smsEntries = smsData.getData();

                    for (SmsEntry smsEntry : smsEntries) {
                        // Access individual SMS entries and their properties here
                        List<TailInstance> smsEntries2 = smsEntry.getTailInstances();
                        for (TailInstance smsEntry2 : smsEntries2)
                        {
                            System.out.println("SMS ID: " + smsEntry2.getSmsId());
                            System.out.println("Message: " + smsEntry2.getMessage());
                        }
                        System.out.println("SMS ID: " + smsEntry.getSmsId());
                        System.out.println("Message: " + smsEntry.getMessage());
                    }

                    // Manually commit the offset after processing the message
                    consumer.commitSync();
                }
            }
        }).start();
        
    }

    public static void main(String[] args) throws InterruptedException {
        InitialiseConsumer("localhost", "9092");
        ReadMessage("telco_gp", 0);
    }
}
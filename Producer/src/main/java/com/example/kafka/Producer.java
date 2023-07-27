package com.example.kafka;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Collections;
import java.util.Properties;

public class Producer
{

    public static Properties properties;
    public static KafkaProducer<String, String> producer;

    static void createProducer(String TopicName, int numberOfPartition, String BootstrapAddress, int Port)
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

    static void dumpMessage(String TopicName, String Message)
    {
        ProducerRecord<String, String> record = new ProducerRecord<>(TopicName, Message);
        int i=0;
        while(i<100) {
            producer.send(record);
            System.out.println("Message sent successfully for topic: " + TopicName);
            i++;
        }
        producer.flush();
    }

    public static void main(String[] args)
    {
        createProducer("telco_gp3",3,"localhost",9092);
        dumpMessage("telco_gp3", "{ \"data\": [ { \"smsId\": \"4831553\", \"callingPartyNumber\": \"8809678123200\", \"tailInstances\": [ { \"smsId\": \"4831555\", \"callingPartyNumber\": \"8809678123200\", \"tailInstances\": null, \"message\": \"ধ্যাপক এব্রো বোয়ার তার 'অটোমান উইমেন ইন পাবলিক স্পেস' বইতে লিখেছেন\", \"encoding\": \"2\", \"parentId\": \"4831553\", \"destinationNumber\": \"8801767876110\" }, { \"smsId\": \"4831556\", \"callingPartyNumber\": \"8809678123200\", \"tailInstances\": null, \"message\": \" যে এই নারীদের মধ্যে অনেকেই শুধু হেরেমের মধ্যে তাদের অস্তিত্ব হারিয়\", \"encoding\": \"2\", \"parentId\": \"4831553\", \"destinationNumber\": \"8801767876110\" }, { \"smsId\": \"4831557\", \"callingPartyNumber\": \"8809678123200\", \"tailInstances\": null, \"message\": \"ে ফেলেননি। তুরস্কের মিডল ইস্ট টেকনিক্যাল ইউনিভার্সিটির আন্তর্জাতিক \", \"encoding\": \"2\", \"parentId\": \"4831553\", \"destinationNumber\": \"8801767876110\" }, { \"smsId\": \"4831558\", \"callingPartyNumber\": \"8809678123200\", \"tailInstances\": null, \"message\": \"সম্পর্কের অধ্যাপক এব্রো বোয়ার তার 'অটোমান উইমেন ইন পাবলিক স্পেস' ব\", \"encoding\": \"2\", \"parentId\": \"4831553\", \"destinationNumber\": \"8801767876110\" }, { \"smsId\": \"4831559\", \"callingPartyNumber\": \"8809678123200\", \"tailInstances\": null, \"message\": \"ইতে লিখেছেন যে এই নারীদের মধ্যে অনেকেই শুধু হেরেমের মধ্যে তাদের অস্\", \"encoding\": \"2\", \"parentId\": \"4831553\", \"destinationNumber\": \"8801767876110\" }, { \"smsId\": \"4831560\", \"callingPartyNumber\": \"8809678123200\", \"tailInstances\": null, \"message\": \"তিত্ব হারিয়ে ফেলেননি। \", \"encoding\": \"2\", \"parentId\": \"4831553\", \"destinationNumber\": \"8801767876110\" } ], \"message\": \"তুরস্কের মিডল ইস্ট টেকনিক্যাল ইউনিভার্সিটির আন্তর্জাতিক সম্পর্কের অ\", \"encoding\": \"2\", \"parentId\": null, \"destinationNumber\": \"8801767876110\" }, { \"smsId\": \"4831552\", \"callingPartyNumber\": \"8809678123200\", \"tailInstances\": [ { \"smsId\": \"4831561\", \"callingPartyNumber\": \"8809678123200\", \"tailInstances\": null, \"message\": \"ধ্যাপক এব্রো বোয়ার তার 'অটোমান উইমেন ইন পাবলিক স্পেস' বইতে লিখেছেন\", \"encoding\": \"2\", \"parentId\": \"4831552\", \"destinationNumber\": \"8801743801850\" }, { \"smsId\": \"4831562\", \"callingPartyNumber\": \"8809678123200\", \"tailInstances\": null, \"message\": \" যে এই নারীদের মধ্যে অনেকেই শুধু হেরেমের মধ্যে তাদের অস্তিত্ব হারিয়\", \"encoding\": \"2\", \"parentId\": \"4831552\", \"destinationNumber\": \"8801743801850\" }, { \"smsId\": \"4831563\", \"callingPartyNumber\": \"8809678123200\", \"tailInstances\": null, \"message\": \"ে ফেলেননি। তুরস্কের মিডল ইস্ট টেকনিক্যাল ইউনিভার্সিটির আন্তর্জাতিক \", \"encoding\": \"2\", \"parentId\": \"4831552\", \"destinationNumber\": \"8801743801850\" }, { \"smsId\": \"4831564\", \"callingPartyNumber\": \"8809678123200\", \"tailInstances\": null, \"message\": \"সম্পর্কের অধ্যাপক এব্রো বোয়ার তার 'অটোমান উইমেন ইন পাবলিক স্পেস' ব\", \"encoding\": \"2\", \"parentId\": \"4831552\", \"destinationNumber\": \"8801743801850\" }, { \"smsId\": \"4831565\", \"callingPartyNumber\": \"8809678123200\", \"tailInstances\": null, \"message\": \"ইতে লিখেছেন যে এই নারীদের মধ্যে অনেকেই শুধু হেরেমের মধ্যে তাদের অস্\", \"encoding\": \"2\", \"parentId\": \"4831552\", \"destinationNumber\": \"8801743801850\" }, { \"smsId\": \"4831566\", \"callingPartyNumber\": \"8809678123200\", \"tailInstances\": null, \"message\": \"তিত্ব হারিয়ে ফেলেননি। \", \"encoding\": \"2\", \"parentId\": \"4831552\", \"destinationNumber\": \"8801743801850\" } ], \"message\": \"তুরস্কের মিডল ইস্ট টেকনিক্যাল ইউনিভার্সিটির আন্তর্জাতিক সম্পর্কের অ\", \"encoding\": \"2\", \"parentId\": null, \"destinationNumber\": \"8801743801850\" } ] }");
    }
}
//{"data":[{"smsId":"000004","callingPartyNumber":"8809678123200","tailInstances":null,"message":"Hi","encoding":"1","parentId":null,"destinationNumber":"8801743801850"}]}
//{ "data": [ { "smsId": "4831553", "callingPartyNumber": "8809678123200", "tailInstances": [ { "smsId": "4831555", "callingPartyNumber": "8809678123200", "tailInstances": null, "message": "ধ্যাপক এব্রো বোয়ার তার 'অটোমান উইমেন ইন পাবলিক স্পেস' বইতে লিখেছেন", "encoding": "2", "parentId": "4831553", "destinationNumber": "8801767876110" }, { "smsId": "4831556", "callingPartyNumber": "8809678123200", "tailInstances": null, "message": " যে এই নারীদের মধ্যে অনেকেই শুধু হেরেমের মধ্যে তাদের অস্তিত্ব হারিয়", "encoding": "2", "parentId": "4831553", "destinationNumber": "8801767876110" }, { "smsId": "4831557", "callingPartyNumber": "8809678123200", "tailInstances": null, "message": "ে ফেলেননি। তুরস্কের মিডল ইস্ট টেকনিক্যাল ইউনিভার্সিটির আন্তর্জাতিক ", "encoding": "2", "parentId": "4831553", "destinationNumber": "8801767876110" }, { "smsId": "4831558", "callingPartyNumber": "8809678123200", "tailInstances": null, "message": "সম্পর্কের অধ্যাপক এব্রো বোয়ার তার 'অটোমান উইমেন ইন পাবলিক স্পেস' ব", "encoding": "2", "parentId": "4831553", "destinationNumber": "8801767876110" }, { "smsId": "4831559", "callingPartyNumber": "8809678123200", "tailInstances": null, "message": "ইতে লিখেছেন যে এই নারীদের মধ্যে অনেকেই শুধু হেরেমের মধ্যে তাদের অস্", "encoding": "2", "parentId": "4831553", "destinationNumber": "8801767876110" }, { "smsId": "4831560", "callingPartyNumber": "8809678123200", "tailInstances": null, "message": "তিত্ব হারিয়ে ফেলেননি। ", "encoding": "2", "parentId": "4831553", "destinationNumber": "8801767876110" } ], "message": "তুরস্কের মিডল ইস্ট টেকনিক্যাল ইউনিভার্সিটির আন্তর্জাতিক সম্পর্কের অ", "encoding": "2", "parentId": null, "destinationNumber": "8801767876110" }, { "smsId": "4831552", "callingPartyNumber": "8809678123200", "tailInstances": [ { "smsId": "4831561", "callingPartyNumber": "8809678123200", "tailInstances": null, "message": "ধ্যাপক এব্রো বোয়ার তার 'অটোমান উইমেন ইন পাবলিক স্পেস' বইতে লিখেছেন", "encoding": "2", "parentId": "4831552", "destinationNumber": "8801743801850" }, { "smsId": "4831562", "callingPartyNumber": "8809678123200", "tailInstances": null, "message": " যে এই নারীদের মধ্যে অনেকেই শুধু হেরেমের মধ্যে তাদের অস্তিত্ব হারিয়", "encoding": "2", "parentId": "4831552", "destinationNumber": "8801743801850" }, { "smsId": "4831563", "callingPartyNumber": "8809678123200", "tailInstances": null, "message": "ে ফেলেননি। তুরস্কের মিডল ইস্ট টেকনিক্যাল ইউনিভার্সিটির আন্তর্জাতিক ", "encoding": "2", "parentId": "4831552", "destinationNumber": "8801743801850" }, { "smsId": "4831564", "callingPartyNumber": "8809678123200", "tailInstances": null, "message": "সম্পর্কের অধ্যাপক এব্রো বোয়ার তার 'অটোমান উইমেন ইন পাবলিক স্পেস' ব", "encoding": "2", "parentId": "4831552", "destinationNumber": "8801743801850" }, { "smsId": "4831565", "callingPartyNumber": "8809678123200", "tailInstances": null, "message": "ইতে লিখেছেন যে এই নারীদের মধ্যে অনেকেই শুধু হেরেমের মধ্যে তাদের অস্", "encoding": "2", "parentId": "4831552", "destinationNumber": "8801743801850" }, { "smsId": "4831566", "callingPartyNumber": "8809678123200", "tailInstances": null, "message": "তিত্ব হারিয়ে ফেলেননি। ", "encoding": "2", "parentId": "4831552", "destinationNumber": "8801743801850" } ], "message": "তুরস্কের মিডল ইস্ট টেকনিক্যাল ইউনিভার্সিটির আন্তর্জাতিক সম্পর্কের অ", "encoding": "2", "parentId": null, "destinationNumber": "8801743801850" } ] }
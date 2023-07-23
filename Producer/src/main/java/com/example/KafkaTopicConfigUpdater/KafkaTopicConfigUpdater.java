package com.example.KafkaTopicConfigUpdater;

import java.util.*;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.clients.admin.ConfigEntry;

import java.util.concurrent.ExecutionException;

public class KafkaTopicConfigUpdater {
    public static void main(String[] args) {
        // Set the properties for AdminClient
        Properties adminClientProps = new Properties();
        adminClientProps.put("bootstrap.servers", "localhost:9092");

        // Topic to update
        String topicName = "telcobright2";

        // New retention time in milliseconds (7 days)
        long retentionTimeMs = 1000;
        long retentionBytes = 100;

        try (AdminClient adminClient = AdminClient.create(adminClientProps)) {
            // Create the configuration entry with the new retention.ms setting
            ConfigEntry retentionConfigEntryT = new ConfigEntry("retention.ms", Long.toString(retentionTimeMs));
            ConfigEntry retentionConfigEntryB = new ConfigEntry("retention.bytes", Long.toString(retentionBytes));


            // Create the configuration change request
            ConfigResource resource = new ConfigResource(Type.TOPIC, topicName);
            AlterConfigOp opT = new AlterConfigOp(retentionConfigEntryT, AlterConfigOp.OpType.SET);
            AlterConfigOp opB = new AlterConfigOp(retentionConfigEntryB, AlterConfigOp.OpType.SET);
            Map<ConfigResource, Collection<AlterConfigOp>> configChangeMapT = Collections.singletonMap(resource, Collections.singleton(opT));
            Map<ConfigResource, Collection<AlterConfigOp>> configChangeMapB = Collections.singletonMap(resource, Collections.singleton(opB));

            // Send the configuration change request to Kafka
            AlterConfigsResult alterConfigsResultT = adminClient.incrementalAlterConfigs(configChangeMapT);
            AlterConfigsResult alterConfigsResultB = adminClient.incrementalAlterConfigs(configChangeMapB);

            alterConfigsResultT.all().get();
            alterConfigsResultB.all().get();

            System.out.println("Retention policy updated successfully.");
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

    }
}

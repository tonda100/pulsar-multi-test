package net.osomahe.pulsarmulti;

import org.apache.pulsar.client.api.*;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@RequestScoped
public class ProducerService {

    @Inject
    @ConfigProperty(name = "pulsar.producer-name")
    String producerName;
    @Inject
    @ConfigProperty(name = "pulsar.tenant")
    String tenant;
    @Inject
    @ConfigProperty(name = "pulsar.namespace")
    String namespace;

    @Inject
    PulsarClient pulsarClient;

    @Inject
    GeneratorService generator;


    public Map<String, List<String>> produceMessages() {
        Map<String, List<String>> data = generator.generateData();
        data.entrySet().forEach(entry -> publish(entry.getKey(), entry.getValue()));
        return data;
    }


    private void publish(String topic, List<String> messages) {
        try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .compressionType(CompressionType.LZ4)
                .enableBatching(true)
                .batchingMaxMessages(100)
                .batchingMaxPublishDelay(1, TimeUnit.SECONDS)
                .producerName(producerName)
                .topic(String.format("%s/%s/%s", tenant, namespace, topic))
                .create()) {
            for (String message : messages) {
                producer.send(message);
            }
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }
}

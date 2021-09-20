package net.osomahe.pulsarmulti;

import org.apache.pulsar.client.api.*;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@RequestScoped
public class ProducerService {
    private static final Logger log = Logger.getLogger(ProducerService.class);

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

    AtomicInteger count = new AtomicInteger(0);


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
                sendMessage(producer, message);
            }
            log.debug(String.format("Produced topic[%s]: %s", count.incrementAndGet(), topic));
            Thread.sleep(1_000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sendMessage(Producer<String> producer, String message) {
        try {
            producer.send(message);
        } catch (PulsarClientException e) {
            log.warn("Trying again", e);
            try {
                Thread.sleep(3_000);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
            sendMessage(producer, message);
        }
    }

    public void produceData(Map<String, List<String>> data) {
        Executors.newSingleThreadExecutor().submit(() -> {
            for (Map.Entry<String, List<String>> entry : data.entrySet()) {
                publish(entry.getKey(), entry.getValue());
            }
        });
    }
}

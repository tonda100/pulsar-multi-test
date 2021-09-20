package net.osomahe.pulsarmulti;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import org.apache.pulsar.client.api.*;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class ConsumerService {

    @Inject
    PulsarClient pulsarClient;

    @Inject
    @ConfigProperty(name = "pulsar.subscriber-name")
    String subscriberName;
    @Inject
    @ConfigProperty(name = "pulsar.tenant")
    String tenant;
    @Inject
    @ConfigProperty(name = "pulsar.namespace")
    String namespace;

    private Consumer<String> consumer;
    @Inject
    StringMessageListener listener;


    void startup(@Observes StartupEvent event) {
        try {
            consumer = pulsarClient.newConsumer(Schema.STRING)
                    .topicsPattern(String.format("%s/%s/.*", tenant, namespace))
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscriptionType(SubscriptionType.Failover)
                    .subscriptionName(subscriberName)
                    .messageListener(listener)
                    .subscribe();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }

    }


    void shutdown(@Observes ShutdownEvent event) {
        try {
            this.consumer.close();
            this.pulsarClient.close();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }


    public Uni<Map<String, List<String>>> getData() {
        return Uni.createFrom().item(listener.data);
    }
}

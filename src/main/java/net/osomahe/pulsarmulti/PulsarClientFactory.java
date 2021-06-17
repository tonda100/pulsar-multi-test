package net.osomahe.pulsarmulti;

import io.quarkus.runtime.ShutdownEvent;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

@ApplicationScoped
public class PulsarClientFactory {

    @Inject
    @ConfigProperty(name = "pulsar.service-url")
    String serviceUrl;

    PulsarClient pulsarClient;

    private void init() {
        try {
            this.pulsarClient = PulsarClient.builder()
                    .serviceUrl(serviceUrl)
                    .build();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }

    @Produces
    public PulsarClient getPulsarClient() {
        if (this.pulsarClient == null) {
            init();
        }
        return this.pulsarClient;
    }

    void shutdown(@Observes ShutdownEvent event) {
        try {
            this.pulsarClient.close();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }

}

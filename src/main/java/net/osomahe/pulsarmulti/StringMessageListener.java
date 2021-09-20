package net.osomahe.pulsarmulti;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClientException;
import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class StringMessageListener implements MessageListener<String> {
    private static final Logger log = Logger.getLogger(StringMessageListener.class);

    static Map<String, List<String>> data = new ConcurrentHashMap<>();

    @Override
    public void received(Consumer<String> consumer, Message<String> message) {
        try {
            String topicName = message.getTopicName().substring(message.getTopicName().lastIndexOf('/') + 1);
            List<String> messages;
            synchronized (data) {
                if (data.containsKey(topicName)) {
                    messages = data.get(topicName);
                } else {
                    messages = new ArrayList<>();
                    data.put(topicName, messages);
                }
            }
            messages.add(message.getValue());
        } catch (Exception e) {
            log.error("Error consuming data.", e);
        } finally {
            try {
                consumer.acknowledge(message);
            } catch (PulsarClientException e) {
                log.error("Error in ack.", e);
            }
        }
    }
}

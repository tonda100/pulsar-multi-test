package net.osomahe.pulsarmulti;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@RequestScoped
public class GeneratorService {

    @Inject
    @ConfigProperty(name = "messages-count")
    Integer messagesCount;

    @Inject
    @ConfigProperty(name = "topics-count")
    Integer topicsCount;


    public Map<String, List<String>> generateData() {
        Map<String, List<String>> data = new ConcurrentHashMap<>();
        for (int t = 0; t < topicsCount; t++) {
            String topic = UUID.randomUUID().toString();
            List<String> messages = new ArrayList<>(messagesCount);
            for (int m = 0; m < messagesCount; m++) {
                messages.add(UUID.randomUUID().toString());
            }
            data.put(topic, messages);
        }
        return data;
    }

}

package net.osomahe.pulsarmulti;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import org.jboss.logging.Logger;

import javax.enterprise.context.control.ActivateRequestContext;
import javax.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestPulsar implements QuarkusApplication {
    private static final Logger log = Logger.getLogger(TestPulsar.class);

    @Inject
    ProducerService producer;

    @Inject
    ConsumerService consumer;

    @Inject
    GeneratorService generator;

    @Override
    @ActivateRequestContext
    public int run(String... args) throws Exception {
        final Map<String, List<String>> data = generator.generateData();

        producer.produceData(data);
        consumer.getData()
                //.onItem().delayIt().by(Duration.ofSeconds(20))
                .onItem().transform(consumed -> check(data, consumed))
                .onFailure().retry()
                .withBackOff(Duration.ofSeconds(10), Duration.ofSeconds(10))
                .atMost(300)
                .onFailure().retry()
                .until(this::shouldRetry)
                .subscribe()
                .with(result -> {
                    if (result) {
                        log.info("DONE");
                        Quarkus.asyncExit(0);
                    }
                });
        Quarkus.waitForExit();
        return 0;
    }

    private boolean shouldRetry(Throwable throwable) {
        return throwable instanceof MessageCheckException;
    }

    private boolean check(Map<String, List<String>> produced, Map<String, List<String>> consumed) {
        log.info("P:" + produced.size() + ", C:" + consumed.size());
        if (produced == null || produced.isEmpty()) {
            throw new MessageCheckException();
        }
        Set<String> pTopics = produced.keySet();
        Set<String> cTopics = consumed.keySet();
        for (String p : pTopics) {
            if (!cTopics.contains(p)) {
                throw new MessageCheckException();
            }
            List<String> pMsg = produced.get(p);
            List<String> cMsg = consumed.get(p);
            for (String pm : pMsg) {
                if (!cMsg.contains(pm)) {
                    throw new MessageCheckException();
                }
            }
        }
        return true;
    }
}

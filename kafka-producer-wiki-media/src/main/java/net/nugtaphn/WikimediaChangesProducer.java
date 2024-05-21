package net.nugtaphn;


import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.util.concurrent.TimeUnit;

@Service
public class WikimediaChangesProducer {

    @Value("${spring.kafka.topic.name}")
    private String topicName;

    private static final Logger LOGGER = LoggerFactory.getLogger(WikimediaChangesProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    public WikimediaChangesProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage() throws InterruptedException {
        BackgroundEventHandler eventHandler = new WikimediaChangesHandler(kafkaTemplate, topicName);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        URI uriUrl = URI.create(url);
        EventSource.Builder builder = new EventSource.Builder(uriUrl);
        BackgroundEventSource.Builder eventSource = new BackgroundEventSource.Builder(eventHandler,builder);
        BackgroundEventSource source = eventSource.build();
        source.start();

        TimeUnit.MINUTES.sleep(10);
    }
}

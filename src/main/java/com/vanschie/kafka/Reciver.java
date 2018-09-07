package com.vanschie.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;

@Component
public class Reciver {

    private static final Logger LOGGER = LoggerFactory.getLogger(Reciver.class);

    private CountDownLatch latch = new CountDownLatch(1);

    public CountDownLatch getLatch() {
        return latch;
    }

    @KafkaListener(topics = "${kafka.topic.receiver}")
    public void receive(String payload) {
        LOGGER.info("received payload='{}'", payload);
        latch.countDown();
    }
}

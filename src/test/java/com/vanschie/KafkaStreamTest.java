package com.vanschie;

import com.vanschie.kafka.Reciver;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;t
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;


@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
public class KafkaStreamTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamTest.class);
    private static final String RECEIVER_TOPIC = "topic";
    public static final String TEST_MESSAGE = "Hello Spring Kafka Receiver!";

    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, RECEIVER_TOPIC);

    private KafkaTemplate<String, String> template;

    private KafkaMessageListenerContainer<String, String> container;

    private BlockingQueue<ConsumerRecord<String, String>> records;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;


    @Before
    public void setUp() throws Exception {
        this.setUpConsumer();
        this.setUpProducer();
    }

    private void setUpProducer() throws Exception {
        // set up the Kafka producer properties
        Map<String, Object> senderProperties =
                KafkaTestUtils.senderProps(embeddedKafka.getBrokersAsString());

        // create a Kafka producer factory
        ProducerFactory<String, String> producerFactory =
                new DefaultKafkaProducerFactory<String, String>(senderProperties);

        // create a Kafka template
        template = new KafkaTemplate<>(producerFactory);
        // set the default topic to send to
        template.setDefaultTopic(RECEIVER_TOPIC);

    }

    private void setUpConsumer() throws Exception {
        // set up the Kafka consumer properties
        Map<String, Object> consumerProperties =
                KafkaTestUtils.consumerProps("sender", "false", embeddedKafka);

        // create a Kafka consumer factory
        DefaultKafkaConsumerFactory<String, String> consumerFactory =
                new DefaultKafkaConsumerFactory<String, String>(consumerProperties);

        // set the topic that needs to be consumed
        ContainerProperties containerProperties = new ContainerProperties(RECEIVER_TOPIC);

        // create a Kafka MessageListenerContainer
        container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);

        // create a thread safe queue to store the received message
        records = new LinkedBlockingQueue<>();

        // setup a Kafka message listener
        container.setupMessageListener(new MessageListener<String, String>() {

            @Override
            public void onMessage(ConsumerRecord<String, String> record) {
                LOGGER.debug("test-listener received message='{}'", record.toString());
                records.add(record);
            }
        });

        // start the container and underlying message listener
        container.start();

        // wait until the container has the required number of assigned partitions
        ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
    }

    @Test
    public void canProduceAndConsumeAMessage() throws InterruptedException {
        template.sendDefault(TEST_MESSAGE);
        LOGGER.debug("test-sender sent message='{}'", TEST_MESSAGE);

        // check that the message was received
        ConsumerRecord<String, String> received = records.poll(10, TimeUnit.SECONDS);

        assertThat(received.value(), is(TEST_MESSAGE));
    }

    @After
    public void tearDown() {
        container.stop();
    }

}

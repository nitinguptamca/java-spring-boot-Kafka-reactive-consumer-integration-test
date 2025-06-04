package com.nkg.prectise.kafka.config;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.nkg.prectise.kafka.domain.Order;
import com.nkg.prectise.kafka.service.OrderService;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Configuration;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    private final KafkaProperties kafkaProperties;
    private final OrderService orderService;
    private final ObjectMapper objectMapper;

    private static final String INPUT_ORDER_TOPIC = "input-orders";
    private static final String GROUP_ID = "order-processor-group";

    public KafkaConsumerConfig(KafkaProperties kafkaProperties, OrderService orderService, ObjectMapper objectMapper) {
        this.kafkaProperties = kafkaProperties;
        this.orderService = orderService;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void init() {
        Disposable disposable = createKafkaReceiver()
                .receive()
                .doOnNext(record -> log.info("Received message: key={}, value={}, topic={}, partition={}, offset={}",
                        record.key(), record.value(), record.topic(), record.partition(), record.offset()))
                .concatMap(record -> {
                    try {
                        Order order = objectMapper.readValue(record.value(), Order.class);
                        order.setKafkaKey(record.key()); // Set Kafka key from message
                        return orderService.processOrder(order)
                                .doOnSuccess(processedOrder -> record.receiverOffset().acknowledge())
                                .onErrorResume(e -> {
                                    log.error("Failed to process order from Kafka record. Key: {}, Value: {}. Error: {}", record.key(), record.value(), e.getMessage());
                                    // Depending on your error handling strategy:
                                    // 1. Acknowledge and move on (if you handle dead-letter queues downstream)
                                    // 2. Do not acknowledge (message will be re-delivered) - careful with infinite loops!
                                    // For this example, we acknowledge to prevent re-processing failed messages in test.
                                    record.receiverOffset().acknowledge();
                                    return Mono.empty(); // Continue processing other messages
                                });
                    } catch (Exception e) {
                        log.error("Error deserializing Kafka message: key={}, value={}. Error: {}", record.key(), record.value(), e.getMessage());
                        record.receiverOffset().acknowledge(); // Acknowledge bad message to avoid reprocessing
                        return Mono.empty(); // Skip this message
                    }
                })
                .doOnError(e -> log.error("Error in Kafka stream: {}", e.getMessage()))
                .subscribe();

        // If you need to stop the consumer cleanly, you would dispose of 'disposable'
        // For a long-running application, you typically don't dispose here.
    }

    private KafkaReceiver<String, String> createKafkaReceiver() {
        Map<String, Object> consumerProps = new HashMap<>(kafkaProperties.buildConsumerProperties());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Important for tests
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // Manual acknowledgment

        ReceiverOptions<String, String> receiverOptions = ReceiverOptions.<String, String>create(consumerProps)
                .subscription(Collections.singleton(INPUT_ORDER_TOPIC));

        return KafkaReceiver.create(receiverOptions);
    }
}

package com.nkg.prectise.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nkg.prectise.kafka.domain.Order;
import com.nkg.prectise.kafka.repository.OrderRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Testcontainers
class KafkaConcatmapIntegrationTest {

    private static final String INPUT_ORDER_TOPIC = "input-orders";
    private static final String ORDER_PROCESSED_TOPIC = "order-processed-events";

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:13.12"))
            .withDatabaseName("testdb")
            .withUsername("user")
            .withPassword("password");

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private ObjectMapper objectMapper;

    private KafkaTemplate<String, String> kafkaTemplate;
    private KafkaReceiver<String, String> orderProcessedEventsReceiver;

    @DynamicPropertySource
    static void setProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create-drop"); // Ensures clean state for each test run
    }

    @BeforeEach
    void setup() {
        // Clear database before each test
        orderRepository.deleteAll();

        // Setup Kafka Producer
        Map<String, Object> producerProps = new HashMap<>(KafkaTestUtils.producerProps(kafka.getBootstrapServers()));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));
        kafkaTemplate.setDefaultTopic(INPUT_ORDER_TOPIC);

        // Setup Kafka Consumer for 'order-processed-events' topic
        Map<String, Object> consumerProps = new HashMap<>(KafkaTestUtils.consumerProps(kafka.getBootstrapServers()));
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group-" + UUID.randomUUID()); // Unique group ID for each test
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // Important for reactive consumer for acknowledgment

        ReceiverOptions<String, String> receiverOptions = ReceiverOptions.<String, String>create(consumerProps)
                .subscription(Collections.singleton(ORDER_PROCESSED_TOPIC));
        orderProcessedEventsReceiver = KafkaReceiver.create(receiverOptions);
    }

    @Test
    void shouldProcessOrderSuccessfullyAndPersistAndPublishEvent() throws Exception {
        // Given
        String orderKey = UUID.randomUUID().toString();
        Order order = new Order("Laptop", 1, orderKey);
        String orderJson = objectMapper.writeValueAsString(order);

        // When
        // Send message to input-orders topic
        kafkaTemplate.send(INPUT_ORDER_TOPIC, orderKey, orderJson).get(10, TimeUnit.SECONDS);
        System.out.println("Sent order message to Kafka: " + orderJson);

        // Then
        // 1. Verify order is saved in the database
        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    List<Order> ordersInDb = orderRepository.findAll();
                    System.out.println("Orders in DB: " + ordersInDb);
                    assertThat(ordersInDb).hasSize(1);
                    Order savedOrder = ordersInDb.get(0);
                    assertThat(savedOrder.getProduct()).isEqualTo("Laptop");
                    assertThat(savedOrder.getQuantity()).isEqualTo(1);
                    assertThat(savedOrder.getStatus()).isEqualTo("PROCESSED");
                    assertThat(savedOrder.getKafkaKey()).isEqualTo(orderKey);
                });

        // 2. Verify an "order processed" event is published to the output topic
        Flux<ConsumerRecord<String, String>> processedRecords = orderProcessedEventsReceiver.receive()
                .doOnNext(rec -> rec.receiverOffset().acknowledge()); // Acknowledge to allow consumer to move on

        List<ConsumerRecord<String, String>> receivedEvents = processedRecords
                .filter(record -> record.key().equals(orderKey))
                .take(1) // Expecting one event for this order
                .collectList()
                .block(Duration.ofSeconds(15)); // Wait for the event

        assertThat(receivedEvents).isNotNull().hasSize(1);
        ConsumerRecord<String, String> processedEvent = receivedEvents.get(0);
        assertThat(processedEvent.key()).isEqualTo(orderKey);
        assertThat(processedEvent.value()).contains("processed successfully");
        System.out.println("Received processed event from Kafka: Key=" + processedEvent.key() + ", Value=" + processedEvent.value());
    }

    @Test
    void shouldHandleInvalidOrderAndSkipProcessing() throws Exception {
        // Given
        String orderKey = UUID.randomUUID().toString();
        // Invalid order with quantity 0
        Order invalidOrder = new Order("Invalid Item", 0, orderKey);
        String invalidOrderJson = objectMapper.writeValueAsString(invalidOrder);

        // When
        kafkaTemplate.send(INPUT_ORDER_TOPIC, orderKey, invalidOrderJson).get(10, TimeUnit.SECONDS);
        System.out.println("Sent invalid order message to Kafka: " + invalidOrderJson);

        // Then
        // Verify no order is saved in the database for this key
        await().atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    List<Order> ordersInDb = orderRepository.findAll();
                    System.out.println("Orders in DB (invalid case): " + ordersInDb);
                    assertThat(ordersInDb).isEmpty(); // No orders should be saved
                });

        // Verify no "order processed" event is published for this invalid order
        Flux<ConsumerRecord<String, String>> processedRecords = orderProcessedEventsReceiver.receive()
                .doOnNext(rec -> rec.receiverOffset().acknowledge());

        List<ConsumerRecord<String, String>> receivedEvents = processedRecords
                .filter(record -> record.key().equals(orderKey))
                .take(Duration.ofSeconds(5)) // Wait for a short period to ensure no event arrives
                .collectList()
                .block(Duration.ofSeconds(6)); // Block longer than take duration to ensure completion

        assertThat(receivedEvents).isNotNull().isEmpty(); // No events should be received for this key
        System.out.println("No processed event received for invalid order, as expected.");
    }
}
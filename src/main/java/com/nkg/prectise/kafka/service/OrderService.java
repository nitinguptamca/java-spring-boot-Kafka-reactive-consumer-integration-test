package com.nkg.prectise.kafka.service;


import com.nkg.prectise.kafka.domain.Order;
import com.nkg.prectise.kafka.repository.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orderRepository;
    private final KafkaTemplate<String, String> kafkaTemplate; // To send "order processed" event

    private static final String ORDER_PROCESSED_TOPIC = "order-processed-events";

    public OrderService(OrderRepository orderRepository, KafkaTemplate<String, String> kafkaTemplate) {
        this.orderRepository = orderRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    public Mono<Order> processOrder(Order order) {
        return Mono.just(order)
                .doOnNext(o -> {
                    // Simulate some validation or complex business logic
                    if (o.getQuantity() <= 0) {
                        throw new IllegalArgumentException("Order quantity must be positive: " + o.getKafkaKey());
                    }
                    log.info("Processing order with key: {}", o.getKafkaKey());
                    o.setStatus("PROCESSED");
                })
                .flatMap(o -> Mono.fromCallable(() -> {
                    Order savedOrder = orderRepository.save(o);
                    log.info("Order saved to database: {}", savedOrder);
                    return savedOrder;
                }))
                .doOnNext(savedOrder -> {
                    // Publish "order processed" event
                    kafkaTemplate.send(ORDER_PROCESSED_TOPIC, savedOrder.getKafkaKey(), "Order " + savedOrder.getId() + " processed successfully.");
                    log.info("Published order processed event for key: {}", savedOrder.getKafkaKey());
                })
                .doOnError(e -> log.error("Error processing order with key: {}. Error: {}", order.getKafkaKey(), e.getMessage()));
    }
}

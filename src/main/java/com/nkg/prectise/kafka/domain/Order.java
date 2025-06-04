package com.nkg.prectise.kafka.domain;


import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.LocalDateTime;
import java.util.Objects;

@Entity
@Table(name = "orders")
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String product;
    private int quantity;
    private String status;
    private LocalDateTime createdAt;
    private String kafkaKey; // To store Kafka message key

    public Order() {
    }

    public Order(String product, int quantity, String kafkaKey) {
        this.product = product;
        this.quantity = quantity;
        this.status = "PENDING";
        this.createdAt = LocalDateTime.now();
        this.kafkaKey = kafkaKey;
    }

    // Getters and Setters
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public String getKafkaKey() {
        return kafkaKey;
    }

    public void setKafkaKey(String kafkaKey) {
        this.kafkaKey = kafkaKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Order order = (Order) o;
        return quantity == order.quantity && Objects.equals(id, order.id) && Objects.equals(product, order.product) && Objects.equals(status, order.status) && Objects.equals(kafkaKey, order.kafkaKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, product, quantity, status, kafkaKey);
    }

    @Override
    public String toString() {
        return "Order{" +
                "id=" + id +
                ", product='" + product + '\'' +
                ", quantity=" + quantity +
                ", status='" + status + '\'' +
                ", createdAt=" + createdAt +
                ", kafkaKey='" + kafkaKey + '\'' +
                '}';
    }
}

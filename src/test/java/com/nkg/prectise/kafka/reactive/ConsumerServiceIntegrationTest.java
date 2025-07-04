package com.nkg.prectise.kafka.reactive;
package com.example.integrationtestspringkafka.service;

import com.example.integrationtestspringkafka.dto.ExampleDTO;
import com.example.integrationtestspringkafka.entity.ExampleEntity;
import com.example.integrationtestspringkafka.repository.ExampleRepository;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Durations;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(SpringExtension.class)
@SpringBootTest
@DirtiesContext
@EmbeddedKafka(topics = {"TOPIC_EXAMPLE", "TOPIC_EXAMPLE_EXTERNE"})
public class ConsumerServiceIntegrationTest {
    Logger log = LoggerFactory.getLogger(ConsumerServiceIntegrationTest.class);

    private static final String TOPIC_EXAMPLE = "TOPIC_EXAMPLE";

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private ExampleRepository exampleRepository;

    public ExampleDTO mockExampleDTO(String name, String description) {
        ExampleDTO exampleDTO = new ExampleDTO();
        exampleDTO.setDescription(description);
        exampleDTO.setName(name);
        return exampleDTO;
    }

    /**
     * We verify the output in the topic. But aslo in the database.
     */
    @Test
    public void itShould_ConsumeCorrectExampleDTO_from_TOPIC_EXAMPLE_and_should_saveCorrectExampleEntity() throws ExecutionException, InterruptedException {
        // GIVEN
        ExampleDTO exampleDTO = mockExampleDTO("Un nom 2", "Une description 2");
        // simulation consumer
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker.getBrokersAsString());
        log.info("props {}", producerProps);
        Producer<String, ExampleDTO> producerTest = new KafkaProducer(producerProps, new StringSerializer(), new JsonSerializer<ExampleDTO>());
        // Or
        // ProducerFactory producerFactory = new DefaultKafkaProducerFactory<String, ExampleDTO>(producerProps, new StringSerializer(), new JsonSerializer<ExampleDTO>());
        // Producer<String, ExampleDTO> producerTest = producerFactory.createProducer();
        // Or
        // ProducerRecord<String, ExampleDTO> producerRecord = new ProducerRecord<String, ExampleDTO>(TOPIC_EXAMPLE, "key", exampleDTO);
        // KafkaTemplate<String, ExampleDTO> template = new KafkaTemplate<>(producerFactory);
        // template.setDefaultTopic(TOPIC_EXAMPLE);
        // template.send(producerRecord);
        // WHEN
        producerTest.send(new ProducerRecord(TOPIC_EXAMPLE, "", exampleDTO));
        // THEN
        // we must have 1 entity inserted
        // We cannot predict when the insertion into the database will occur. So we wait until the value is present. Thank to Awaitility.
        await().atMost(Durations.TEN_SECONDS).untilAsserted(() -> {
            var exampleEntityList = exampleRepository.findAll();
            assertEquals(1, exampleEntityList.size());
            ExampleEntity firstEntity = exampleEntityList.get(0);
            assertEquals(exampleDTO.getDescription(), firstEntity.getDescription());
            assertEquals(exampleDTO.getName(), firstEntity.getName());
        });
        producerTest.close();
    }
}
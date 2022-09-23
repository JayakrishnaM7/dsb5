package com.ibm.org;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@SpringBootTest(properties = {"spring.cloud.config.enabled=false", "spring.profiles.active=test"})
@EmbeddedKafka(partitions = 1)
public class FundretriveInitRsTest {
    
    
    @Value("${spring.kafka.template.InitRsProcessor-input}")
    private String inputTopic;
    @Value("${spring.kafka.template.InitRsProcessor-output1}")
    private String outputTopic1;

    private BlockingQueue<ConsumerRecord<String, Object>> recordsQueue = new LinkedBlockingQueue<>();

    @AfterEach
    public void tearDown() throws Exception {

    }

    @Test
    public void testExecute() {
        //Send test message using kafkaTemplate
        //listen for responses
        /**
        try {
            ConsumerRecord<String,Object> record = recordsQueue.poll(10000, TimeUnit.MILLISECONDS);
            //cast record.value() to appropriate type
            //use appropriate Assertions functions to test response
        } catch (InterruptedException ex) {
            Assertions.assertFalse(false);
        }
        **/
    }

    /**
     * Use below listener/s to listen to response for sent test messages
     * TODO: be sure to use appropriate containerFactory. The one used here is for representational purpose
    **/ 
    @KafkaListener(topics = "${spring.kafka.template.InitRsProcessor-output1}", 
            containerFactory = "kafkaListenerContainerFactory", groupId = "test-consumer")
    public void listenTOoutputTopic1(ConsumerRecord<?,?> record) {
        recordsQueue.add((ConsumerRecord<String, Object>) record);
    }
}

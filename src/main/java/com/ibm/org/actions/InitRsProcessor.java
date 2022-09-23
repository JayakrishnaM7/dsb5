package com.ibm.org.actions;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;


@Service
@Configuration
public class InitRsProcessor 
{        

	private final Logger log = LoggerFactory.getLogger(this.getClass());
        
        
	@Value("${spring.kafka.template.InitRsProcessor-input}")
	private String inputTopic;
        
        @Value("${spring.kafka.template.InitRsProcessor-output1}")
	private String retriveRSTopic;

        @Autowired
        private KafkaTemplate<String, byte[]> output1Template;
  

	
	private final Counter counter = Metrics.counter(
                            "Number.of.messages.consumed", 
                            "topic", 
                            "spring.kafka.template.InitRsProcessor-input");
	private final Timer timer = Metrics.timer(
                            "response_time_milliseconds", 
                            "method", 
                            "InitRsProcessor.processMessages");

	/**
	 *  This method is used to listen messages from Kafka topic. 
	 * 
	 * @param data
	 * @throws IOException
	 */
	@KafkaListener(id = "spring.kafka.template.InitRsProcessor-input", 
            topics = "${spring.kafka.template.InitRsProcessor-input}", 
            containerFactory = "kafkaListenerContainerFactory")
	public void processMessages(ConsumerRecord<String, byte[]> record) throws IOException 
        {

		long start = System.currentTimeMillis();
		log.info("Message received from {} topic" , inputTopic);
                
                counter.increment();
                byte[] message = record.value();
                /**
                 * @TODO: Write your business logic here
                */
                byte[] responseBytes = null;
                ProducerRecord responseRecord = new ProducerRecord<>(retriveRSTopic,"add_keyName_here", responseBytes);
                record.headers().forEach(header -> responseRecord.headers().add(header));
                this.output1Template.send(responseRecord);
		
		Long elapsedTime = System.currentTimeMillis() - start;
		timer.record(elapsedTime, TimeUnit.MILLISECONDS);
	}
}


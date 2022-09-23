package com.ibm.org.configurations;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@EnableKafka
@Configuration
public class KafkaProducerFactory {

	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapserver;	
	@Value("${spring.kafka.producer.linger-ms}")
	private String lingerMSConfig;
	@Value("${spring.kafka.producer.client-id}")
	private String clientId;
	@Value("${spring.kafka.producer.buffer-memory}")
	private String bufferMemory;
	@Value("${spring.kafka.producer.acks}")
	private String acks;

        
	@Bean
	public ProducerFactory<String, byte[]> producerFactory() {
		return new DefaultKafkaProducerFactory<>(producerConfigs());
	}

	@Bean
	public Map<String, Object> producerConfigs() {

		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapserver);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId); 
		props.put(ProducerConfig.LINGER_MS_CONFIG,lingerMSConfig);
		
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
		props.put(ProducerConfig.ACKS_CONFIG, acks);		
		 
		return props;
	}

	@Bean
	public KafkaTemplate<String, byte[]> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}
}

package com.windowforsun.kafka.json.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import com.windowforsun.kafka.json.event.InboundKey;
import com.windowforsun.kafka.json.event.InboundPayload;
import com.windowforsun.kafka.json.service.DemoService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class DemoConsumer {
	private final DemoService demoService;

	@KafkaListener(topics = "#{'${demo.inboundTopic}'}",
	// @KafkaListener(topics = "${demo.inboundTopic}",
		groupId = "demo-consumer-group",
		containerFactory = "kafkaListenerContainerFactory",
		errorHandler = "errorHandler")
	public void listen(@Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) InboundKey key, @Payload InboundPayload payload) {
		log.info("Received message - key primaryId: " + key.getId() + " - secondaryId: " + key.getTopic() + " - payload: " + payload);

		try {
			this.demoService.process(key, payload);
		} catch (Exception e) {
			log.error("Error processing message: " + e.getMessage());
		}
	}
}

package com.windowforsun.kafka.json.producer;


import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import com.windowforsun.kafka.json.event.OutboundKey;
import com.windowforsun.kafka.json.event.OutboundPayload;
import com.windowforsun.kafka.json.properties.DemoProperties;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class DemoProducer {
	private final DemoProperties properties;
	private final KafkaTemplate<Object, Object> kafkaTemplate;

	public SendResult<OutboundKey, OutboundPayload> sendMessage(OutboundKey key, OutboundPayload payload) throws
		Exception {
		try {
			SendResult<OutboundKey, OutboundPayload> result = (SendResult) this.kafkaTemplate.send(this.properties.getOutboundTopic(), key, payload).get();
			log.info("Emitted message - key: " + key + " id: " + payload.getId() + " - payload: " + payload.getData());

			return result;
		} catch (Exception e) {
			String message = "Error sending message to topic " + properties.getOutboundTopic();
			log.error(message);

			throw new Exception(message, e);
		}
	}
}

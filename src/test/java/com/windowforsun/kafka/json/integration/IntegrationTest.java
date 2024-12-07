package com.windowforsun.kafka.json.integration;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import com.windowforsun.kafka.json.DemoConfig;
import com.windowforsun.kafka.json.event.InboundKey;
import com.windowforsun.kafka.json.event.InboundPayload;
import com.windowforsun.kafka.json.event.OutboundKey;
import com.windowforsun.kafka.json.event.OutboundPayload;
import com.windowforsun.kafka.json.properties.DemoProperties;
import com.windowforsun.kafka.json.util.Util;

import lombok.extern.slf4j.Slf4j;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

@Slf4j
@SpringBootTest(classes = DemoConfig.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@ActiveProfiles("test")
@EmbeddedKafka(controlledShutdown = true)
// @EmbeddedKafka(controlledShutdown = true, topics = {"#{'${demo.inboundTopic}'}", "#{'${demo.outboundTopic}'}"})
// @EmbeddedKafka(controlledShutdown = true, topics = {"${demo.inboundTopic}", "${demo.outboundTopic}"})
public class IntegrationTest {
	@Autowired
	private KafkaTemplate<Object, Object> kafkaTemplate;
	@Autowired
	private EmbeddedKafkaBroker embeddedKafka;
	@Autowired
	private KafkaListenerEndpointRegistry registry;
	@Autowired
	private KafkaTestListener testListener;
	@Autowired
	private DemoProperties properties;

	@Configuration
	static class TestConfig {
		@Bean
		public KafkaTestListener testListener() {
			return new KafkaTestListener();
		}
	}

	public static class KafkaTestListener {
		AtomicInteger counter = new AtomicInteger(0);

		List<ImmutablePair<OutboundKey, OutboundPayload>> keyedMessages = new ArrayList<>();

		@KafkaListener(groupId = "kafkaTestGroup",
			topics = "#{'${demo.outboundTopic}'}",
			autoStartup = "true")
		public void listen(@Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) OutboundKey key, @Payload final OutboundPayload payload, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
			log.debug("KafkaTestListener - Received message: id: " + payload.getId() + " - outbound data: " + payload.getData() + " - key: " + key.getId());
			log.info("testListener topic : {}", topic);
			assertThat(key, notNullValue());
			assertThat(payload, notNullValue());
			keyedMessages.add(ImmutablePair.of(key, payload));
			counter.incrementAndGet();
		}
	}

	@BeforeEach
	public void setUp() {
		this.registry.getListenerContainers()
			.stream()
			.forEach(container -> ContainerTestUtils.waitForAssignment(container, this.embeddedKafka.getPartitionsPerTopic()));
		this.testListener.counter.set(0);
		this.testListener.keyedMessages = new ArrayList<>();
	}

	@Test
	public void testSuccess_SingleEvent() throws Exception {
		UUID keyId = UUID.randomUUID();
		InboundKey inboundKey = Util.buildInboundKey(keyId, this.properties.getInboundTopic());
		UUID payloadId = UUID.randomUUID();
		InboundPayload inboundPayload = Util.buildInboundPayload(payloadId);

		this.kafkaTemplate.send(this.properties.getInboundTopic(), inboundKey, inboundPayload).get();

		Awaitility.await().atMost(1, TimeUnit.SECONDS)
			.pollDelay(100, TimeUnit.MILLISECONDS)
			.until(this.testListener.counter::get, is(1));

		assertThat(this.testListener.keyedMessages, hasSize(1));
		assertThat(this.testListener.keyedMessages.get(0).getLeft().getId(), is(keyId));
		assertThat(this.testListener.keyedMessages.get(0).getRight().getData(), is("processed:" + inboundPayload.getData()));
	}

	@Test
	public void testSuccess_MultipleEvents() throws Exception {
		int totalMessages = 10;

		for(int i = 0; i < totalMessages; i++) {
			UUID keyId = UUID.randomUUID();
			InboundKey inboundKey = Util.buildInboundKey(keyId, this.properties.getInboundTopic());
			UUID payloadId = UUID.randomUUID();
			InboundPayload inboundPayload = Util.buildInboundPayload(payloadId);
			this.kafkaTemplate.send(this.properties.getInboundTopic(), inboundKey, inboundPayload).get();
		}

		Awaitility.await().atMost(5, TimeUnit.SECONDS)
			.pollDelay(100, TimeUnit.MILLISECONDS)
			.until(this.testListener.counter::get, is(totalMessages));
	}

	@Test
	public void testSkip_InvalidMessageFormat() throws Exception {
		int totalMessages = 10;

		for(int i = 0; i < totalMessages; i++) {

			if(i % 2 == 0) {

				UUID keyId = UUID.randomUUID();
				InboundKey inboundKey = Util.buildInboundKey(keyId, this.properties.getInboundTopic());
				UUID payloadId = UUID.randomUUID();
				InboundPayload inboundPayload = Util.buildInboundPayload(payloadId);
				this.kafkaTemplate.send(this.properties.getInboundTopic(), inboundKey, inboundPayload).get();
			} else {
				this.kafkaTemplate.send(this.properties.getInboundTopic(), "key:" + i, "payload:" + i).get();
			}
		}

		Awaitility.await().atMost(5, TimeUnit.SECONDS)
			.pollDelay(100, TimeUnit.MILLISECONDS)
			.until(this.testListener.counter::get, is(totalMessages / 2));
	}
}


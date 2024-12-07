package com.windowforsun.kafka.json.producer;

import static org.mockito.Mockito.*;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

import com.windowforsun.kafka.json.event.OutboundKey;
import com.windowforsun.kafka.json.event.OutboundPayload;
import com.windowforsun.kafka.json.properties.DemoProperties;
import com.windowforsun.kafka.json.util.Util;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.*;

public class DemoProducerTest {
	private DemoProperties mockDemoProperties;
	private KafkaTemplate mockKafkaTemplate;
	private DemoProducer demoProducer;

	@BeforeEach
	public void setUp() {
		this.mockDemoProperties = mock(DemoProperties.class);
		this.mockKafkaTemplate = mock(KafkaTemplate.class);
		this.demoProducer = new DemoProducer(this.mockDemoProperties, this.mockKafkaTemplate);
	}

	@Test
	public void testSendMessage_success() throws Exception {
		String topic = "test-outbound-topic";
		OutboundKey key = Util.buildOutboundKey(UUID.randomUUID());
		OutboundPayload payload = Util.buildOutboundPayload(UUID.randomUUID());

		when(this.mockDemoProperties.getOutboundTopic()).thenReturn(topic);
		ListenableFuture futureResult = mock(ListenableFuture.class);
		SendResult expectedSendResult = mock(SendResult.class);
		when(futureResult.get()).thenReturn(expectedSendResult);
		when(this.mockKafkaTemplate.send(topic, key, payload)).thenReturn(futureResult);

		SendResult result = this.demoProducer.sendMessage(key, payload);

		verify(this.mockKafkaTemplate, times(1)).send(topic, key, payload);
		assertThat(result, is(expectedSendResult));
	}

	@Test
	public void testSendMessage_ExceptionOnSend() throws Exception {
		OutboundKey key = Util.buildOutboundKey(UUID.randomUUID());
		OutboundPayload payload = Util.buildOutboundPayload(UUID.randomUUID());
		String topic = "test-outbound-topic";

		when(this.mockDemoProperties.getOutboundTopic()).thenReturn(topic);
		ListenableFuture futureResult = mock(ListenableFuture.class);
		when(this.mockKafkaTemplate.send(topic, key, payload)).thenReturn(futureResult);

		doThrow(new ExecutionException("send fail", new Exception("fail"))).when(futureResult).get();

		Exception exception = Assertions.assertThrows(Exception.class,
			() -> this.demoProducer.sendMessage(key, payload));

		verify(this.mockKafkaTemplate, times(1)).send(topic, key, payload);
		assertThat(exception.getMessage(), is("Error sending message to topic " + topic));
	}
}

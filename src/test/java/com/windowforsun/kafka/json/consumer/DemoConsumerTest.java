package com.windowforsun.kafka.json.consumer;

import static org.mockito.Mockito.*;

import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.windowforsun.kafka.json.event.InboundKey;
import com.windowforsun.kafka.json.event.InboundPayload;
import com.windowforsun.kafka.json.service.DemoService;
import com.windowforsun.kafka.json.util.Util;

public class DemoConsumerTest {
	private DemoService mockDemoService;
	private DemoConsumer demoConsumer;

	@BeforeEach
	public void setUp() {
		this.mockDemoService = mock(DemoService.class);
		this.demoConsumer = new DemoConsumer(this.mockDemoService);
	}

	@Test
	public void testListen_Success() throws Exception {
		String topic = "test-inbound-topic";
		InboundKey key = Util.buildInboundKey(UUID.randomUUID(), topic);
		InboundPayload payload = Util.buildInboundPayload(UUID.randomUUID());

		this.demoConsumer.listen(key, payload);

		verify(this.mockDemoService, times(1)).process(key, payload);
	}

	@Test
	public void testListen_serviceThrowsException() throws Exception {
		String topic = "test-inbound-topic";
		InboundKey key = Util.buildInboundKey(UUID.randomUUID(), topic);
		InboundPayload payload = Util.buildInboundPayload(UUID.randomUUID());

		doThrow(new RuntimeException("fail")).when(this.mockDemoService)
			.process(key, payload);

		this.demoConsumer.listen(key, payload);

		verify(this.mockDemoService, times(1)).process(key, payload);
	}
}

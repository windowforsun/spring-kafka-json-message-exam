package com.windowforsun.kafka.json.service;

import static org.mockito.Mockito.*;

import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.windowforsun.kafka.json.event.InboundKey;
import com.windowforsun.kafka.json.event.InboundPayload;
import com.windowforsun.kafka.json.event.OutboundKey;
import com.windowforsun.kafka.json.event.OutboundPayload;
import com.windowforsun.kafka.json.producer.DemoProducer;
import com.windowforsun.kafka.json.util.Util;

public class DemoServiceTest {
	private DemoProducer mockDemoProducer;
	private DemoService demoService;

	@BeforeEach
	public void setUp() {
		this.mockDemoProducer = mock(DemoProducer.class);
		this.demoService = new DemoService(mockDemoProducer);
	}

	@Test
	public void testProcess() throws Exception {
		InboundKey key = Util.buildInboundKey(UUID.randomUUID(), "testTopic");
		InboundPayload payload = Util.buildInboundPayload(UUID.randomUUID());

		this.demoService.process(key, payload);

		verify(this.mockDemoProducer, times(1)).sendMessage(any(OutboundKey.class), any(OutboundPayload.class));
	}
}

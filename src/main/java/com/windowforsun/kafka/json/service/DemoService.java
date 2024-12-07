package com.windowforsun.kafka.json.service;

import org.springframework.stereotype.Service;

import com.windowforsun.kafka.json.event.InboundKey;
import com.windowforsun.kafka.json.event.InboundPayload;
import com.windowforsun.kafka.json.event.OutboundKey;
import com.windowforsun.kafka.json.event.OutboundPayload;
import com.windowforsun.kafka.json.producer.DemoProducer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class DemoService {
	private final DemoProducer demoProducer;

	public void process(InboundKey key, InboundPayload payload) throws Exception {
		OutboundPayload outboundPayload = OutboundPayload.builder()
			.id(payload.getId())
			.inboundData(payload.getData())
			.data("processed:" + payload.getData())
			.build();

		OutboundKey outboundKey = OutboundKey.builder()
			.id(key.getId())
			.build();

		this.demoProducer.sendMessage(outboundKey, outboundPayload);
	}
}

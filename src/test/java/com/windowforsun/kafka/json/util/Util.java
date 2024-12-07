package com.windowforsun.kafka.json.util;

import java.util.UUID;

import com.windowforsun.kafka.json.event.InboundKey;
import com.windowforsun.kafka.json.event.InboundPayload;
import com.windowforsun.kafka.json.event.OutboundKey;
import com.windowforsun.kafka.json.event.OutboundPayload;

public class Util {
	public static String INBOUND_DATA = "inbound data";
	public static String OUTBOUND_DATA = "outbound data";

	public static InboundKey buildInboundKey(UUID id, String topic) {
		return InboundKey.builder()
			.id(id)
			.topic(topic)
			.build();
	}

	public static InboundPayload buildInboundPayload(UUID id) {
		return InboundPayload.builder()
			.id(id)
			.data(INBOUND_DATA)
			.build();
	}

	public static OutboundKey buildOutboundKey(UUID id) {
		return OutboundKey.builder()
			.id(id)
			.build();
	}

	public static OutboundPayload buildOutboundPayload(UUID id) {
		return OutboundPayload.builder()
			.id(id)
			.data(OUTBOUND_DATA)
			.build();
	}
}

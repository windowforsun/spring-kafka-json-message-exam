package com.windowforsun.kafka.json.event;

import java.util.UUID;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OutboundPayload {
	private UUID id;
	private String inboundData;
	private String data;
}

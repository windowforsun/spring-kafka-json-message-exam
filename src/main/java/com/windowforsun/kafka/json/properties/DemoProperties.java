package com.windowforsun.kafka.json.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Data;

@Configuration
@ConfigurationProperties("demo")
@Data
public class DemoProperties {
	private String outboundTopic;
	private String inboundTopic;
}

package com.micro.streaming.analytics;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

@Component
@PropertySource("classpath:config/application.properties")
@ConfigurationProperties(prefix = "analytics")
public class AnalyticsProperties {
	
	private String queueIN;

	public String getQueueIN() {
		return queueIN;
	}

	public void setQueueIN(String queueIN) {
		this.queueIN = queueIN;
	}
	
}

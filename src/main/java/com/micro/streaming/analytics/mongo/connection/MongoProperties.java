package com.micro.streaming.analytics.mongo.connection;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

@Component
@PropertySource("classpath:config/application.properties")
@ConfigurationProperties(prefix = "spring.data.mongodb.database")
public class MongoProperties {
	
	private Boolean test;
	private String host;
	private String testHost;
	private Integer port;	
	private String database;
	private String username;
	private String password;
	private String authenticationDatabase;
	private String southCollectCollection;
	private String provisionCollectCollection;
	
	public String getHost() {
		return this.test!=null&&this.test==true?this.testHost:host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public Integer getPort() {
		return port;
	}
	public void setPort(Integer port) {
		this.port = port;
	}
	public String getDatabase() {
		return database;
	}
	public void setDatabase(String database) {
		this.database = database;
	}
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public String getSouthCollectCollection() {
		return southCollectCollection;
	}
	public void setSouthCollectCollection(String southCollectCollection) {
		this.southCollectCollection = southCollectCollection;
	}
	public String getProvisionCollectCollection() {
		return provisionCollectCollection;
	}
	public void setProvisionCollectCollection(String provisionCollectCollection) {
		this.provisionCollectCollection = provisionCollectCollection;
	}
	public String getAuthenticationDatabase() {
		return authenticationDatabase;
	}
	public void setAuthenticationDatabase(String authenticationDatabase) {
		this.authenticationDatabase = authenticationDatabase;
	}
	public Boolean getTest() {
		return test;
	}
	public void setTest(Boolean test) {
		this.test = test;
	}
	public String getTestHost() {
		return testHost;
	}
	public void setTestHost(String testHost) {
		this.testHost = testHost;
	}
		
}

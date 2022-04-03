package com.micro.streaming.analytics.mongo.connection;

import org.bson.Document;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

@Configuration
public class MongoConfiguration {
	
	private MongoProperties mongoProperties;
	
	public MongoConfiguration(MongoProperties mongoProperties) {
		this.mongoProperties = mongoProperties;
	}

	@Bean
	public MongoClient mongoClient() {

		MongoClient mongoClient = null;		
		
		if(mongoProperties.getPort()!=null && mongoProperties.getPort()>0 
				&& mongoProperties.getHost()!=null && !mongoProperties.getHost().isEmpty()) {
			
			MongoCredential mongoCredential = MongoCredential.createCredential(
					mongoProperties.getUsername(), 
					mongoProperties.getDatabase(), 
					mongoProperties.getPassword().toCharArray()
			);
			MongoClientOptions options = MongoClientOptions.builder()
	                .connectionsPerHost(10)
	                .socketTimeout(10000)
	                .maxWaitTime(10000)
	                .connectTimeout(10000)
	                .build();
			ServerAddress serverAddresses = new ServerAddress(mongoProperties.getHost(), mongoProperties.getPort());

			if(mongoProperties.getUsername().isEmpty()) 
				mongoClient = new MongoClient(serverAddresses);
			else
				mongoClient = new MongoClient(serverAddresses, mongoCredential, options);			
		}
		
		return mongoClient;
	}
	
	@Bean
	public MongoDatabase mongoDatabase(MongoClient mongoClient) {
		
		MongoDatabase database = mongoClient.getDatabase(mongoProperties.getDatabase());
		
		return database;
	}
	
	@Bean
	public MongoCollection<Document> southCollectCollection(MongoDatabase mongoDatabase) {
		
		MongoCollection<Document> collection = mongoDatabase.getCollection(mongoProperties.getSouthCollectCollection());
		
		return collection;
	}
	
	@Bean
	public MongoCollection<Document> provisionCollectCollection(MongoDatabase mongoDatabase) {
		
		MongoCollection<Document> collection = mongoDatabase.getCollection(mongoProperties.getProvisionCollectCollection());
		
		return collection;
	}
}

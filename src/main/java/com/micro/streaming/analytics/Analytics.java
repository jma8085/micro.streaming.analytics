package com.micro.streaming.analytics;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import javax.annotation.PostConstruct;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import com.micro.streaming.analytics.mongo.MongoCollectionFields;
import com.micro.streaming.analytics.mongo.MongoDao;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

/**
 * Hello world!
 *
 */
@SpringBootApplication
@ComponentScan({"com.micro.streaming.analytics.mongo"})
public class Analytics {
	
	private final static String QUEUE_NAME = "hello";
	
	@Autowired
	private MongoDao mongoDao;
	
	public static void main(String[] args) throws IOException, TimeoutException {
		
		System.out.println("Hello World!");
		
		SpringApplication.run(Analytics.class, args);
		
	}
	
	@PostConstruct
	public void action() throws IOException, TimeoutException {
		
		ConnectionFactory factory = new ConnectionFactory();
		
	    factory.setHost("localhost");
	    
	    Connection connection = factory.newConnection();
	    Channel channel = connection.createChannel();

	    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
	    
	    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
	    
	    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
	        String message = new String(delivery.getBody(), "UTF-8");
	        System.out.println("Received '" + message + "'");

//	        mongoDao.saveData("asset1", 17.50);
			
			Document result = mongoDao.getAvgTemeperatureById("asset1");
			
			if(result!=null && result.getDouble(MongoCollectionFields.otuput_avg)!=null) {
				System.out.println(" AVG Temperature " + result.getDouble(MongoCollectionFields.otuput_avg).toString());
			}
	    };
	    
	    channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });
	}
	
}

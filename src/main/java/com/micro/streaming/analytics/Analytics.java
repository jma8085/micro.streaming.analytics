package com.micro.streaming.analytics;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeoutException;

import javax.annotation.PostConstruct;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;

import com.github.wnameless.json.flattener.JsonFlattener;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.micro.streaming.analytics.model.IOTData;
import com.micro.streaming.analytics.mongo.MongoCollectionFields;
import com.micro.streaming.analytics.mongo.MongoDao;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

@SpringBootApplication(exclude={MongoAutoConfiguration.class, MongoDataAutoConfiguration.class})
@ComponentScan({"com.micro.streaming.analytics.mongo", "com.micro.streaming.analytics"})
public class Analytics {
	
	@Autowired
	private MongoDao mongoDao;
	
	@Autowired
	private AnalyticsProperties analyticsProperties;
	
	public static void main(String[] args) throws IOException, TimeoutException {		
		
		SpringApplication.run(Analytics.class, args);		
	}
	
	@PostConstruct
	public void action() throws IOException, TimeoutException {
		
		ConnectionFactory factory = new ConnectionFactory();
		
	    factory.setHost(analyticsProperties.getRabbitHost());
	    
	    Connection connection = factory.newConnection();
	    Channel channel = connection.createChannel();

	    channel.queueDeclare(analyticsProperties.getQueueIN(), false, false, false, null);
	    
	    System.out.println("Waiting for messages...");
	    
	    @SuppressWarnings("unchecked")
		DeliverCallback deliverCallback = (consumerTag, delivery) -> {
	        String message = new String(delivery.getBody(), "UTF-8");
	        System.out.println("Received '" + JsonFlattener.flatten(message) + "'");
	        IOTData<Double> iotData = null;
	        
	        try {
	        	iotData = new Gson().fromJson(message, IOTData.class);
	        	
	        } catch (JsonParseException jsonEx) {
	        	System.out.println("Received message mal formed");
	        }

	        if(iotData.getDevice()==null 
	        		|| iotData.getDatastreams()==null
	        		|| iotData.getDatastreams().isEmpty()
	        		|| iotData.getDatastreams().get(0).getDatapoints()==null
	        		|| iotData.getDatastreams().get(0).getDatapoints().isEmpty()) {
	        	System.out.println("Received message without measure");
	        }
	        
        	String device = iotData.getDevice();        	
	        iotData.getDatastreams().forEach(datastream -> {
	        	datastream.getDatapoints()
	        		.forEach(datapoint -> {	      
	        			mongoDao.saveIOTData(device, datastream.getId(), datastream.getFeed(), datapoint.getValue());
	        		});
    		});
			
			Document result = mongoDao.getAvgTemeperatureById(device);
			
			if(result!=null && result.getDouble(MongoCollectionFields.otuput_avg)!=null) {
				Double avg = result.getDouble(MongoCollectionFields.otuput_avg);
				mongoDao.provisionData(device, avg, new Date(System.currentTimeMillis()));
				
				System.out.println("Device: " + device + "\nAVG provisioned temperature: " + avg.toString() + "\n");
			
			} else {
				System.out.println("There is some error to provisioned AVG temperature");
			}
	    };
	    
	    channel.basicConsume(analyticsProperties.getQueueIN(), true, deliverCallback, consumerTag -> { });
	}
	
	
}

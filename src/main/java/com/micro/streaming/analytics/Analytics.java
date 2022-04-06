package com.micro.streaming.analytics;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
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
			Document median = mongoDao.getMedianTemperature(device);	
			Document mode = mongoDao.getModeTemperature(device);
			Document stDev = mongoDao.getStandardDesviationTemperature(device);
			Map<Long, Double> quartiles = mongoDao.getQuartilesTemperature(device);
			Document measures = new Document();
			
			if(result!=null) {
				Double avg = result.getDouble(MongoCollectionFields.output_avg);
				Double max = result.getDouble(MongoCollectionFields.output_max);
				Double min = result.getDouble(MongoCollectionFields.output_min);
				
				measures = new Document(MongoCollectionFields.device, device)
					.append(MongoCollectionFields.date, new Date(System.currentTimeMillis()))
					.append(MongoCollectionFields.values, 
						new Document(MongoCollectionFields.output_avg, avg)
							.append(MongoCollectionFields.output_max, max)
							.append(MongoCollectionFields.output_min, min));
			}
			
			if(median != null) {
				measures.append(MongoCollectionFields.output_median, median.getDouble(MongoCollectionFields.value));
			}
			
			if(mode != null) {
				measures.append(MongoCollectionFields.output_median, mode.getDouble(MongoCollectionFields._id));
			}
			
			if(stDev != null) {
				measures.append(MongoCollectionFields.output_median, stDev.getDouble(MongoCollectionFields.output_tempStDev));
			}
			
			if(quartiles!=null && !quartiles.isEmpty()) {				
				measures.append("quartiles", 
						new Document("Q1", quartiles.get(1L))
						.append("Q2", quartiles.get(2L))
						.append("Q3", quartiles.get(3L)));
				
				Document provision = new Document(MongoCollectionFields.temperature, measures);

				mongoDao.provisionData(provision);
				
				System.out.println("Device: " + device + "\nObject provisioned temperature: " + measures.toString() + "\n");
			
			} else {
				System.out.println("There is some error to provisioned AVG temperature");
			}
	    };
	    
	    channel.basicConsume(analyticsProperties.getQueueIN(), true, deliverCallback, consumerTag -> { });
	}
	
	
}
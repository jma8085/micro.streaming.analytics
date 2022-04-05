package com.micro.streaming.analytics;

import static org.junit.Assert.assertNotNull;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.micro.streaming.analytics.mongo.MongoDao;
import com.micro.streaming.analytics.mongo.connection.MongoConfiguration;
import com.micro.streaming.analytics.mongo.connection.MongoProperties;
import com.micro.streaming.analytics.mongo.impl.MongoDaoImpl;


@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(
	classes = {
		MongoConfiguration.class, MongoDaoImpl.class
	},
	webEnvironment=WebEnvironment.NONE)
@EnableConfigurationProperties({MongoProperties.class})
public class MongoIntegrationTest  {

	private static final String device = "asset1";
	
	@Autowired
	private MongoDao mongoDao;
	
	@Before
	public void before() {		
		mongoDao.saveIOTData(device, "temperature", "feed_t", 18.6);			
		mongoDao.saveIOTData(device, "temperature", "feed_t", 22.6);			
		mongoDao.saveIOTData(device, "temperature", "feed_t", 56.6);			
	}
	
	@Test
	public void sampleTest() {		
		assertNotNull(mongoDao.getAvgTemeperatureById(device));
	}

}

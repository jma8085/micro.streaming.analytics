package com.micro.streaming.analytics.mongo;

import java.util.Date;

import org.bson.Document;

public interface MongoDao {
	
	
	public Document getAvgTemeperatureById(String id);
	
	public void saveIOTData(String device, String datastreamId, String feed, Double temperature);
	
	public void provisionData(String device, Double value, Date date);

}

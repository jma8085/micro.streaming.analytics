package com.micro.streaming.analytics.mongo;

import org.bson.Document;

public interface MongoDao {
	
	
	public Document getAvgTemeperatureById(String id);
	
	public void saveData(String device, String datastreamId, String feed, Double temperature);

}

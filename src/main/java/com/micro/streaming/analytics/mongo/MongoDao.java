package com.micro.streaming.analytics.mongo;

import java.util.Map;

import org.bson.Document;

public interface MongoDao {
	
	
	public Document getAvgTemeperatureById(String id);
	
	public Document getMedianTemperature(String id);
	
	public Document getModeTemperature(String id);
	
	public Document getStandardDesviationTemperature(String id);
	
	public Map<Long, Double> getQuartilesTemperature(String id);
	
	public void saveIOTData(String device, String datastreamId, String feed, Double temperature);
	
	public void provisionData(Document document);

}

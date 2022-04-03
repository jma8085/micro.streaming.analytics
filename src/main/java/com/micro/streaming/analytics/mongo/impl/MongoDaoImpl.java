package com.micro.streaming.analytics.mongo.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.micro.streaming.analytics.mongo.MongoActions;
import com.micro.streaming.analytics.mongo.MongoCollectionFields;
import com.micro.streaming.analytics.mongo.MongoDao;
import com.mongodb.client.MongoCollection;

@Component
public class MongoDaoImpl implements MongoDao {
	
	private static final String DATAPOINT_VERSION = "1.0.0";

	@Autowired
	private MongoCollection<Document> southCollectCollection;
	
	@Autowired
	private MongoCollection<Document> provisionCollectCollection;

	@Override
	public Document getAvgTemeperatureById(String id) {
		
		Document pathDatastreams = new Document("path", MongoCollectionFields.$datastreams);
		Document pathDatapoints = new Document("path", "$datastreams.datapoints");
		Document unwindDatastreams = new Document(MongoActions.$unwind, pathDatastreams);
		Document unwindDatapoints = new Document(MongoActions.$unwind, pathDatapoints);
		Document avgTemperature = new Document(MongoActions.$avg, "$datastreams.datapoints.value");
		Document fieldId = new Document(MongoCollectionFields._id, MongoCollectionFields.$device);
		Document groupByDevice = new Document(MongoActions.$group, new Document(fieldId).append("avg", avgTemperature));
		
		List<Document> mongoQuery = new ArrayList<Document>();
		mongoQuery.add(unwindDatastreams);
		mongoQuery.add(unwindDatapoints);
		mongoQuery.add(groupByDevice);
		
		List<Document> lista = new ArrayList<Document>();
		southCollectCollection.aggregate(mongoQuery).forEach((Consumer<Document>) elem -> lista.add(elem));
		
		return lista!=null&&!lista.isEmpty()?lista.get(0):null;
	}

	@Override
	public void saveIOTData(String device, String datastreamId, String feed, Double temperature) {
		
		Document version = new Document(MongoCollectionFields.version, DATAPOINT_VERSION);
		
		List<Document> datapoints = new ArrayList<Document>();
		datapoints.add(new Document(MongoCollectionFields.value, temperature));		
		Document datastream = new Document(MongoCollectionFields.datapoints, datapoints);
		
		if(datastreamId != null) {
			datastream.append(MongoCollectionFields.id, datastreamId);
		}
		
		if(feed != null) {
			datastream.append(MongoCollectionFields.feed, feed);
		}
		
		List<Document> datastreams = new ArrayList<Document>();
		datastreams.add(datastream);		
		Document data = new Document(version)
				.append(MongoCollectionFields.device, device)
				.append(MongoCollectionFields.datastreams, datastreams);
		
		southCollectCollection.insertOne(data);
	}

	@Override
	public void provisionData(String device, Double value, Date date) {
		
		Document temperature = new Document(MongoCollectionFields.device, device)
				.append(MongoCollectionFields.date, date)
				.append(MongoCollectionFields.value, value);
		Document provision = new Document(MongoCollectionFields.temperature, temperature);
		
		provisionCollectCollection.insertOne(provision);
		
	}

}

package com.micro.streaming.analytics.mongo.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.assertj.core.util.Arrays;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.micro.streaming.analytics.mongo.MongoActions;
import com.micro.streaming.analytics.mongo.MongoCollectionFields;
import com.micro.streaming.analytics.mongo.MongoDao;
import com.mongodb.client.MongoCollection;

@Component
public class MongoDaoImpl implements MongoDao {
	
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
	public void saveData(String device, Double temperature) {
		
		Document version = new Document(MongoCollectionFields.version, "1.0.0");
		
		List<Object> datapoints = Arrays.asList(new Document(MongoCollectionFields.value, temperature));		
		Document id = new Document(MongoCollectionFields.id, device);		
		Document datastream = new Document(id)
				.append(MongoCollectionFields.feed, "feed_t")
				.append(MongoCollectionFields.datapoints, datapoints);
		List<Object> datastreams = Arrays.asList(datastream);		
		Document data = new Document(version)
				.append(MongoCollectionFields.datastreams, datastreams);
		
		southCollectCollection.insertOne(data);
	}

}

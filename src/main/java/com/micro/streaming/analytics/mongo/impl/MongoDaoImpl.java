package com.micro.streaming.analytics.mongo.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.micro.streaming.analytics.mongo.MongoActions;
import com.micro.streaming.analytics.mongo.MongoCollectionFields;
import com.micro.streaming.analytics.mongo.MongoDao;
import com.mongodb.client.FindIterable;
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
		Document maxTemperature = new Document(MongoActions.$max, "$datastreams.datapoints.value");
		Document minTemperature = new Document(MongoActions.$min, "$datastreams.datapoints.value");
		Document fieldId = new Document(MongoCollectionFields._id, MongoCollectionFields.$device);
		Document matchDevice = new Document(MongoActions.$match, new Document(MongoCollectionFields.device, id));
		Document groupByDevice = new Document(MongoActions.$group, new Document(fieldId)
				.append(MongoCollectionFields.output_avg, avgTemperature)
				.append(MongoCollectionFields.output_min, minTemperature)
				.append(MongoCollectionFields.output_max, maxTemperature));
		
		List<Document> mongoQuery = new ArrayList<Document>();
		mongoQuery.add(unwindDatastreams);
		mongoQuery.add(unwindDatapoints);
		mongoQuery.add(matchDevice);
		mongoQuery.add(groupByDevice);
		
		List<Document> lista = new ArrayList<Document>();
		southCollectCollection.aggregate(mongoQuery).forEach((Consumer<Document>) elem -> lista.add(elem));
		
		return lista!=null&&!lista.isEmpty()?lista.get(0):null;
	}
	
	@Override
	public Document getMedianTemperature(String id) {
		
		Document filterDevice = new Document(MongoCollectionFields.device, id);
		Document sortValue = new Document("datastreams.datapoints.value", 1L);
		Long numDatastreams = southCollectCollection.countDocuments(filterDevice);
		Document queryMode = new Document(filterDevice);
	
		Integer div = null;
		
		if(numDatastreams%2==0) {
			div = new Long(numDatastreams/2).intValue();
			
		} else {
			div = new Long(numDatastreams/2).intValue()+1;
		}		

		FindIterable<Document> limit = southCollectCollection.find(queryMode).sort(sortValue).skip(div).limit(1);
		
		return limit!=null?limit.first():null;
	}
	
	@Override
	public Document getModeTemperature(String id) {
		
		Document pathDatastreams = new Document("path", MongoCollectionFields.$datastreams);
		Document pathDatapoints = new Document("path", MongoCollectionFields.$datastreams_datapoints);
		Document unwindDatastreams = new Document(MongoActions.$unwind, pathDatastreams);
		Document unwindDatapoints = new Document(MongoActions.$unwind, pathDatapoints);
		Document sumCountTemperature = new Document(MongoActions.$sum, 1);
		Document fieldId = new Document(MongoCollectionFields._id, MongoCollectionFields.$datastreams_datapoints_value);
		Document matchDevice = new Document(MongoActions.$match, new Document(MongoCollectionFields.device, id));
		Document groupByDevice = new Document(MongoActions.$group, new Document(fieldId)
				.append(MongoCollectionFields.output_count, sumCountTemperature));
		Document sortCounters = new Document(MongoActions.$sort, new Document(MongoCollectionFields.output_count, -1));
		
		List<Document> mongoQuery = new ArrayList<Document>();
		mongoQuery.add(unwindDatastreams);
		mongoQuery.add(unwindDatapoints);
		mongoQuery.add(matchDevice);
		mongoQuery.add(groupByDevice);
		mongoQuery.add(sortCounters);
		
		List<Document> lista = new ArrayList<Document>();
		southCollectCollection.aggregate(mongoQuery).forEach((Consumer<Document>) elem -> lista.add(elem));
		
		return lista!=null&&!lista.isEmpty()?lista.get(0):null;
	}
	
	@Override
	public Document getStandardDesviationTemperature(String id) {
		
		Document pathDatastreams = new Document("path", MongoCollectionFields.$datastreams);
		Document pathDatapoints = new Document("path", MongoCollectionFields.$datastreams_datapoints);
		Document unwindDatastreams = new Document(MongoActions.$unwind, pathDatastreams);
		Document unwindDatapoints = new Document(MongoActions.$unwind, pathDatapoints);
		Document stDevTemperature = new Document(MongoActions.$stdDevSamp, MongoCollectionFields.$datastreams_datapoints_value);
		Document fieldId = new Document(MongoCollectionFields._id, MongoCollectionFields.$device);
		Document matchDevice = new Document(MongoActions.$match, new Document(MongoCollectionFields.device, id));
		Document groupByDevice = new Document(MongoActions.$group, new Document(fieldId)
				.append(MongoCollectionFields.output_tempStDev, stDevTemperature));
		
		List<Document> mongoQuery = new ArrayList<Document>();
		mongoQuery.add(unwindDatastreams);
		mongoQuery.add(unwindDatapoints);
		mongoQuery.add(matchDevice);
		mongoQuery.add(groupByDevice);
		
		List<Document> lista = new ArrayList<Document>();
		southCollectCollection.aggregate(mongoQuery).forEach((Consumer<Document>) elem -> lista.add(elem));
		
		return lista!=null&&!lista.isEmpty()?lista.get(0):null;
	}
	
	@Override
	public Map<Long, Double> getQuartilesTemperature(String id) {
		
		FindIterable<Document> firstQuartil = null, firstNextQuartil = null, secondQuartil = null, 
				thirdQuartil = null, thirdNextQuartil = null, secondNextQuartil = null;
		Map<Long, Double> quartiles = new HashMap<Long, Double>();
		Double first = null, second = null, third = null;
		Document filterDevice = new Document(MongoCollectionFields.device, id);
		Document maxMinValue = new Document("datastreams.datapoints.value", 1L);
		Document minMaxValue = new Document("datastreams.datapoints.value", -1L);
		Document queryMode = new Document(filterDevice);

		Long numDocuments = southCollectCollection.countDocuments(queryMode);
		Long middle = numDocuments%2;
		
		if(middle == 0) {
			firstQuartil = southCollectCollection.find(queryMode).sort(maxMinValue).skip(new Long((numDocuments/2)-1).intValue()).limit(1);
			secondQuartil = southCollectCollection.find(queryMode).sort(maxMinValue).skip(new Long((numDocuments/2)-1).intValue()).limit(1);
			secondNextQuartil = southCollectCollection.find(queryMode).sort(maxMinValue).skip(new Long(numDocuments/2).intValue()).limit(1);
			thirdQuartil = southCollectCollection.find(queryMode).sort(minMaxValue).skip(new Long((numDocuments/2)-1).intValue()).limit(1);
			
			Double firstQ = firstQuartil.first().getList(MongoCollectionFields.datastreams, Document.class).get(0)
					.getList(MongoCollectionFields.datapoints, Document.class).get(0)
					.getDouble(MongoCollectionFields.value);
			first = firstQ!=null?firstQ:null;	
			
			Double secondQ = secondQuartil.first().getList(MongoCollectionFields.datastreams, Document.class).get(0)
					.getList(MongoCollectionFields.datapoints, Document.class).get(0)
					.getDouble(MongoCollectionFields.value);
			Double secondNextQ = secondNextQuartil.first().getList(MongoCollectionFields.datastreams, Document.class).get(0)
					.getList(MongoCollectionFields.datapoints, Document.class).get(0)
					.getDouble(MongoCollectionFields.value);
			second = secondQ!=null&&secondNextQ!=null?(secondQ+secondNextQ)/2:null;
			
			Double thirdQ = thirdQuartil.first().getList(MongoCollectionFields.datastreams, Document.class).get(0)
					.getList(MongoCollectionFields.datapoints, Document.class).get(0)
					.getDouble(MongoCollectionFields.value);
			third = thirdQ!=null?thirdQ:null;
			
		} else {
			firstQuartil = southCollectCollection.find(queryMode).sort(maxMinValue).skip(new Long((numDocuments/2)-1).intValue()).limit(1);
			firstNextQuartil = southCollectCollection.find(queryMode).sort(maxMinValue).skip(new Long(numDocuments/2).intValue()).limit(1);
			secondQuartil = southCollectCollection.find(queryMode).sort(maxMinValue).skip(new Long(numDocuments/2).intValue()).limit(1);
			thirdQuartil = southCollectCollection.find(queryMode).sort(minMaxValue).skip(new Long((numDocuments/2)-1).intValue()).limit(1);
			thirdNextQuartil = southCollectCollection.find(queryMode).sort(minMaxValue).skip(new Long(numDocuments/2).intValue()).limit(1);
			
			Double firstQ = firstQuartil.first().getList(MongoCollectionFields.datastreams, Document.class).get(0)
					.getList(MongoCollectionFields.datapoints, Document.class).get(0)
					.getDouble(MongoCollectionFields.value);
			Double firstNextQ = firstNextQuartil.first().getList(MongoCollectionFields.datastreams, Document.class).get(0)
					.getList(MongoCollectionFields.datapoints, Document.class).get(0)
					.getDouble(MongoCollectionFields.value);
			first = firstQ!=null&&firstNextQ!=null?(firstQ+firstNextQ)/2:null;
			
			Double secondQ = secondQuartil.first().getList(MongoCollectionFields.datastreams, Document.class).get(0)
					.getList(MongoCollectionFields.datapoints, Document.class).get(0)
					.getDouble(MongoCollectionFields.value);
			second = secondQ!=null?secondQ:null;		
			
			Double thirdQ = thirdQuartil.first().getList(MongoCollectionFields.datastreams, Document.class).get(0)
					.getList(MongoCollectionFields.datapoints, Document.class).get(0)
					.getDouble(MongoCollectionFields.value);
			Double thirdNextQ = thirdNextQuartil.first().getList(MongoCollectionFields.datastreams, Document.class).get(0)
					.getList(MongoCollectionFields.datapoints, Document.class).get(0)
					.getDouble(MongoCollectionFields.value);
			third = thirdQ!=null&&thirdNextQ!=null?(thirdQ+thirdNextQ)/2:null;
		}
		
		quartiles.put(1L, first);
		quartiles.put(2L, second);
		quartiles.put(3L, third);
		
		return quartiles;
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
	public void provisionData(Document document) {
		
		provisionCollectCollection.insertOne(document);
		
	}

}
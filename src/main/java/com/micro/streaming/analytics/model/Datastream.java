package com.micro.streaming.analytics.model;

import java.util.List;

public class Datastream<T> {
	
	private String id;
	private String feed;
	private List<Datapoint<T>> datapoints;
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getFeed() {
		return feed;
	}
	public void setFeed(String feed) {
		this.feed = feed;
	}
	public List<Datapoint<T>> getDatapoints() {
		return datapoints;
	}
	public void setDatapoints(List<Datapoint<T>> datapoints) {
		this.datapoints = datapoints;
	}

	
}

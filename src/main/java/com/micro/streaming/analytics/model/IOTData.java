package com.micro.streaming.analytics.model;

import java.util.List;

public class IOTData<T> {
	
	private String version;
	private String device;
	private List<Datastream<T>> datastreams;
	
	public String getVersion() {
		return version;
	}
	public void setVersion(String version) {
		this.version = version;
	}
	public List<Datastream<T>> getDatastreams() {
		return datastreams;
	}
	public void setDatastreams(List<Datastream<T>> datastreams) {
		this.datastreams = datastreams;
	}
	public String getDevice() {
		return device;
	}
	public void setDevice(String device) {
		this.device = device;
	}	

}

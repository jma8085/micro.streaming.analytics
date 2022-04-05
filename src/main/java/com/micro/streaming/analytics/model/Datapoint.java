package com.micro.streaming.analytics.model;

public class Datapoint<T> {
	
	private T value;

	public T getValue() {
		return value;
	}

	public void setValue(T value) {
		this.value = value;
	}


}

package org.kafka.streams.processor.api;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.processor.AbstractProcessor;

public class NumberDoubleProcessor extends AbstractProcessor<String, String> {

	@Override
	public void process(String key, String value) {
		System.out.println("NumberDouble processor:: double the value=" + value);
		int v = Integer.valueOf(value);
		v = v * 2;
		context().forward(key, String.valueOf(v));
		context().commit();
	}

}
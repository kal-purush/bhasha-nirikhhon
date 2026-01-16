package com.hemebiotech.analytics;

import java.util.List;
import java.util.Map;

public class MainProgram {

	public static void main(String[] args) throws Exception{
	
		final String file = "symptoms.txt";
		
		AnalyticsCounter analyticsCounter = new AnalyticsCounter();
		
		List<String> symptoms = analyticsCounter.reading(file);
		Map<String, Integer> mapSymptomsOccurences = analyticsCounter.count(symptoms);
		analyticsCounter.saving(mapSymptomsOccurences);

	}

}
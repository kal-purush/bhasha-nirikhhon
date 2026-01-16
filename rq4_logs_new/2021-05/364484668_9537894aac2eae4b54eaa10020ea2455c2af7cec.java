package in.casekartin.service;

import java.util.HashMap;

public class CaseManager {
	
	/**
	 * Create the hash map for storing case types
	 * In hash map CaseType as a key and cost as a value
	 */
	private static HashMap<String, Float> caseTypes = new HashMap<String, Float>();
	static {
		// add the case types with  to the hash Map
		caseTypes.put("POLYCARBONATE", 300.0f);
		caseTypes.put("LEATHER", 500.0f);
		caseTypes.put("SILICONE", 200.0f);
		caseTypes.put("HARD", 499.0f);
		caseTypes.put("FRIENDS NAME TAG GLASS", 600.0f);
		caseTypes.put("CSK CUSTOM", 599.0f);
		caseTypes.put("NEON SAND GLOW", 749.0f);	
	}
	
	/**
	 * Return the case Types 
	 * @return
	 */
	public static HashMap<String,Float> getCaseTypes()
	{
		return caseTypes;
		
	}

}
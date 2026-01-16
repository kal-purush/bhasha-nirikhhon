package in.casekartin.service;

import java.util.HashSet;

import java.util.Set;

import in.casekartin.model.CaseManagerModel;

public class CaseManagerService {
	
	/**
	 * Create the hash map for storing case types
	 * In hash map CaseType as a key and cost as a value
	 */
	private static Set<CaseManagerModel> caseTypes = new HashSet<CaseManagerModel>();
	static {
		
		// add the case types with  to the hash Map
		CaseManagerModel case1=new CaseManagerModel("POLYCARBONATE", 300.0f);
		caseTypes.add(case1);
		CaseManagerModel case2=new CaseManagerModel("LEATHER", 500.0f);
		caseTypes.add(case2);
		CaseManagerModel case3=new CaseManagerModel("SILICONE", 200.0f);
		caseTypes.add(case3);
		CaseManagerModel case4=new CaseManagerModel("HARD", 499.0f);
		caseTypes.add(case4);
		CaseManagerModel case5=new CaseManagerModel("FRIENDS NAME TAG GLASS", 600.0f);
		caseTypes.add(case5);
		CaseManagerModel case6=new CaseManagerModel("CSK CUSTOM", 599.0f);
		caseTypes.add(case6);
		CaseManagerModel case7=new CaseManagerModel("NEON SAND GLOW", 749.0f);
		caseTypes.add(case7);

	}
	
	/**
	 * Return the case Types 
	 * @return 
	 * @return
	 */
	public static  Set<CaseManagerModel> getCaseTypes()
	{
		return caseTypes;
		
	}

}
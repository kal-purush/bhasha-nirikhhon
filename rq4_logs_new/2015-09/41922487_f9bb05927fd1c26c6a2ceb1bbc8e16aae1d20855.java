package com.ewt.dashboardpoint.service.context;
//Change
import com.ewt.dashboardpoint.entity.context.EntityContext;



public class ServiceContext extends EntityContext {

	private static String CLASS_NAME = ServiceContext.class.getName();
	/**
	 * Flag to denote if the current context is a JUNIT context
	 */
	private static boolean isJunitContext = false;

	/**
	 * Flag to denote if the current context is a LocalHost context
	 */
	private static boolean isLocalhostContext = false;
	
	/**
	 * Default Application Id for DASHBOARDPOINT Application
	 */
	//TODO
	private static final String STR_DEFAULT_APP_ID = "";
	
	/**
	 * Default User Id for DASHBOARBPOINT Application
	 */
	//TODO
	private static final String STR_DEFAULT_USER_ID = "";

	/**
	 * @return the isLocalhostContext
	 */
	public static boolean isLocalhostContext() {
		final String STR_LOCALHOST = "localhost";

		boolean isLocalhostContext = true;

		// Check if the Virtual Host Name is defined, if its not, this thread is
		// running on LocalHost
	
//er		if (!STR_LOCALHOST.equalsIgnoreCase(ApplicationPropertiesManager
//er				.getProperty(ApplicationConstants.VIRTUALHOST_NAME))) {

//er			isLocalhostContext = false;
//er		}
		return isLocalhostContext;
	}

	
	/**
	 * Static initializer block to ascertain if the current context is a JUNIT
	 * context or a LocalHost context
	 */
	static {
		// Set if JUNIT context
		isJunitContext = isJunitContext();

		// Set if LocalHost context
		isLocalhostContext = isLocalhostContext();
	}
	
	/**
	 * @return the strDefaultAppId
	 */
	public static String getStrDefaultAppId() {
		return STR_DEFAULT_APP_ID;
	}

	/**
	 * @return the strDefaultUserId
	 */
	public static String getStrDefaultUserId() {
		return STR_DEFAULT_USER_ID;
	}
	
	
	
	/**
	 * Method to check if the current thread is running within the JUNIT context
	 * 
	 * @return
	 */
	private static boolean isJunitContext() {
		final String STR_TRUE = "true";

		return STR_TRUE.equals(System.getProperty("JUnitDerbyTesting"));
	}
	

}
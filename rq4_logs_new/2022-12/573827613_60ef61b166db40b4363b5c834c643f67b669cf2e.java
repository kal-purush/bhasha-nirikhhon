package constants;

public class Constant {
	
	public class FileReaderConstant {
		
		public static final String CONFIGURATION_FILE = "config.properties";
		public static final String USERNAME = "username";
		public static final String PASSWORD = "password";
		public static final String URL = "url";
		public static final String IMPLICIT_WAIT = "implicitWait";
		public static final String EXPLICIT_WAIT = "explicitWait";
		public static final String BROWSER = "browser";
		public static final String ENVIRONMENT = "environment";
		public static final String WINDOW_MAXIMIZED = "windowMaximize";
		
		public static final String CHROME = "chrome";
		public static final String FIREFOX = "firefox";
		public static final String IEXPLORER = "iexplorer";
		
		public static final String LOCAL = "local";
		public static final String REMOTE = "remote";
	}
	
	public class BasePageConstant {
		
		public static final String DATE_FORMAT = "dd-MM-yyyy HH-mm-ss-SSS";
	}
	
	public class AnalysisConstant {
		
		public static final String ORDER_TO_CASH = "OrderToCash";
		public static final String PURCHASE_TO_PAY = "PurchaseToPay";
		public static final String SERVICE_NOW_TICKETING = "ServiceNowTicketing";
	}
	
	public class ServiceNowTicketingConstant {
		
		public static final String METRICS_TILE_XPATH = "//*[text() = 'Metrics']"
				+ "/ancestor::*[@class = 'fullscreen-app__section']"
				+ "//*[@tiles = 'processOverviewGeneral.tiles']/*";
		public static final String HAPPY_PATH_TILE_XPATH = "//*[text() = 'Happy path']"
				+ "/ancestor::*[@class = 'fullscreen-app__section']"
				+ "//*[@class = 'fullscreen-app__section__body']"
				+ "//*[@class = 'row']/*";
		public static final String OTHER_FREQ_TILE_XPATH = "//*[contains(text(), 'Other frequent')]"
				+ "/ancestor::*[@class = 'fullscreen-app__section']"
				+ "//*[@class = 'fullscreen-app__section__body']"
				+ "//*[@class = 'row']/*";
		
		public static final String TILE_HEADER_XPATH_EXT = "//*[contains(@class, 'tile__header')]";
		public static final String TILE_BODY_XPATH_EXT = "//*[contains(@class, 'tile__body')]";
		public static final String TILE_FOOTER_XPATH_EXT = "//*[contains(@class, 'tile__footer')]";
	}
	
	public class PurchaseToPayConstant {
		
		public static final String METRICS_TILE_XPATH = "//*[contains(text(), 'Detected Deviations')]"
				+ "/ancestor::*[@class = 'fullscreen-app__section']"
				+ "//*[@class = 'row']/*";
		
		public static final String TILE_HEADER_XPATH_EXT = "//*[contains(@class, 'tile__header')]";
		public static final String CASE_COUNT_XPATH_EXT = "//*[@title = 'Case count']";
		public static final String ACTIVITIES_COUNT_XPATH_EXT = "//*[@title = 'Activities count']";
	}
}
package common;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import org.apache.log4j.Logger;
import utils.ExcelReader;

//Extend this class for API functionalities
public class SetUp_API {
	
		
		public static Properties APIConfig ;
		public static Properties WOID = Setup_Page.WOID;
		
		public static FileInputStream fis;
	
		public static Logger logs = Logger.getLogger(SetUp_API.class);
		
		

		public SetUp_API() {
			
			if(APIConfig==null) {
				APIConfig = new Properties();
			
			try {
				fis = new FileInputStream(System.getProperty("user.dir")+"/src/test/resources/properties/APIConfig.properties");
				
			} catch (FileNotFoundException e) {
				
				e.printStackTrace();
			}
			try {
				APIConfig.load(fis);
			} catch (IOException e) {
				
				e.printStackTrace();
			}
			logs.debug("Loaded the Config property file");
			if(WOID==null) {
				WOID=new Properties();
			try {
				fis = new FileInputStream(System.getProperty("user.dir")+"/src/test/resources/properties/WOID.properties");
				
			} catch (FileNotFoundException e) {
				
				e.printStackTrace();
			}
			
			try {
				WOID.load(fis);
			} catch (IOException e) {
				
				e.printStackTrace();
			}
			logs.debug("Loaded the WOID property file");
			}
			
			
		}
		
		}
}
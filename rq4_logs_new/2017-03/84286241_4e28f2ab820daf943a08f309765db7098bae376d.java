package dataprovider;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Properties;

public class ConfigDataProvider {
	Properties prop;

	public ConfigDataProvider(){
		
		File src= new File("./Configuration/config.properties");
		try {
			FileInputStream fis = new FileInputStream(src);

			prop = new Properties();
			prop.load(fis);


		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public String getChromePath(){
		String chromepath=prop.getProperty("chromePath");
		return chromepath;
	}
	public String getFirefoxPath(){
		String firepath=prop.getProperty("firefoxPath");
		return firepath;
	}
	public String getApplicationUrl(){
		String url=prop.getProperty("url");
		return url;
	}

}
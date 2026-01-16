package ampeg.tests;

import static org.junit.Assert.*;

import java.util.HashMap;

import javax.print.attribute.HashAttributeSet;

import org.junit.Test;

import framework.ConfigurationProvider;

public class TestConfigurationProvider {

	@Test
	public void test(){
		String file = "config.properties";
		HashMap<String, String> configFileProperties;
		try {
			configFileProperties= new ConfigurationProvider().getPropertiesFromResourceFile(file);
		} catch (Exception e) {
			throw new Run
		}
		
		//("config.properties");
	}

}
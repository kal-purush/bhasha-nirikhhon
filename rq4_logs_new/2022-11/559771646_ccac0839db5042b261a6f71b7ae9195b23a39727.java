package com.sgtesting.objectmapdemo;

import org.openqa.selenium.By;

public class LoginLogoutDemo {

	public static void main(String[] args) {
		
		try
		{
			String filename=".\\objectmap\\objectmap.properties";
			ObjectMap om=new ObjectMap(filename);
	
			By by=om.getLocator("homepageflyoutwin");
		}catch (Exception e) {
			e.printStackTrace();
		}

	}

}
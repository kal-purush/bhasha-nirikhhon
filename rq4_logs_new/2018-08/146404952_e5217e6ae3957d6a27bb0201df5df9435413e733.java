package com.bookmyroom.pages;

import org.openqa.selenium.WebDriver;

import com.bookmyroom.util.Constants;
import com.bookmyroom.util.GetExcelData;
import com.bookmyroom.util.Util;

public class LoginPage {
	
	Util util;
	GetExcelData excelData;
	
	public LoginPage(WebDriver driver){
	util=new Util(driver);
	}
	
	public void login(String email,String password)
	{
		String data[][];
		
		data = excelData.getData(Constants.loginExcelPath, "Login");
		
		for(int i=0;i<data.length;i++)
        {
        	switch(data[i][Constants.keyword_col])
        	{
        	
        	case "navigateTo":
        		util.navigateTo(data[i][Constants.parameter_col]);
        		break;
        		
        	case "enterEmail":
        		util.doAction(data[i][Constants.pathType_col], data[i][Constants.path_col], email);
        		break;
        		
        	case "enterPassword":
        		util.doAction(data[i][Constants.pathType_col], data[i][Constants.path_col], password);
        		break;
        	
        	case "clickSubmit":
        		util.selectAndClick(data[i][Constants.path_col]);
        		break;
        		
        	case "verify":
        		util.verify(data[i][Constants.path_col], data[i][Constants.parameter_col]);  
        		break;
        	
        	}
        }
	
	}

}
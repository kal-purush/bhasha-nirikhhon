package com.pack.test;

import static com.pack.test.AppiumTestBase.getAndroidDriver;

import java.io.IOException;

import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import com.pack.base.AppiumServer;

public class ApiTest {
	AppiumServer as;
	AppiumTestBase ab;
@Test
public void run(){
	 as=new AppiumServer();
	 ab=new AppiumTestBase();
	try {
		as.appiumStart();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	ab.initializeAndroidDriverApiDemos();

}
@AfterTest
public void tearDown(){
	getAndroidDriver().quit();

	try {
		as.appiumStop();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
		}
}
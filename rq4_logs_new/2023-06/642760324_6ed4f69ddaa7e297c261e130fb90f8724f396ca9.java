package com.adactin.finaExe;

import java.util.Date;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class FinalExecutionJunit extends HotelProjectExe{
	@BeforeClass
	public static void bc()
	{
		launchingBrowser("chrome");
		urllaunch(propRead("URL"));
		maximizeWindowSize();
	}
	@Test
	public void Test1() {
		Date d=new Date();
		username();
		password();
		login();
			}
	@Test
	public void Test2()
	{
		location();
		hotels();
		roomType();
		noOfRooms();
		checkIn();
		checkOut();
		adults();
		children();
		search();
	}
	@After
	public void Test3() {
		clickRadioButton();
		continueButton();
	}
	@AfterClass
	public static void Test4() {
		firstName();
		lastName();
		billingAddress();
		cCNo();
		cCType();
		expiryDate();
		expiryYear();
		cvvNumber();
		bookNow();
		implicitWait(10000);
		orderNo();
	}
	
}
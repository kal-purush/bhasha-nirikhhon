package com.hex.smart.desktop.pageobjectlibrary;

import java.io.File;
import java.io.IOException;
import java.net.*;
import org.openqa.selenium.By;
import org.openqa.selenium.winium.DesktopOptions;
import org.openqa.selenium.winium.WiniumDriver;
//import org.openqa.selenium.winium.WiniumDriverService;
//
//import com.hex.smart.desktop.pagelocators.CalculatorPageLocators;

import net.serenitybdd.core.pages.PageObject;

public class CalculatorPageObject extends PageObject {

	public static WiniumDriver driver;

	public static void startWiniumServer() throws Exception {
		Runtime runTime = Runtime.getRuntime();
		String executablePath = "D:\\Winium.Desktop.Driver.exe";
		runTime.exec(executablePath);

	}

	public static void getApplicationPath() throws Exception {
		Thread.sleep(5000);
		DesktopOptions options = new DesktopOptions();
		options.setApplicationPath("C:\\Windows\\System32\\calc.exe");
		driver = new WiniumDriver(new URL("http://localhost:9999"), options);
	}

	public void getDisplayText() throws Exception {
		Thread.sleep(2000);
		String result = driver.findElement(By.id("CalculatorResults")).getAttribute("Name");
		System.out.print(result);
	}




	//		options.setDebugConnectToRunningApp(true);
	//		WiniumDriverService service = new WiniumDriverService.Builder().usingDriverExecutable(new File("D:\\Winium.Desktop.Driver.exe")).usingPort(9999).withVerbose(true).withSilent(false).buildDesktopService();


	//		service.start(); 

	//		driver.findElement(By.id("num9Button")).click();
	//		driver.findElement(By.name("num6Button")).click();


}
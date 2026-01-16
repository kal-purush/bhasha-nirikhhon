package com.sgtesting.tests.testcases;



import org.testng.Assert;

import static org.testng.Assert.assertEquals;

import org.openqa.selenium.Alert;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;




public class Assignment1 
{
	public static WebDriver oBrowser=null;

		@Test(priority=1)
		private static void launchbrowser()
		{
			try
			{

				System.setProperty("webdriver.chrome.driver", "D:\\GitRepository\\CurrentWorkSpace\\ExampleAugust26th2022Repository\\DemoMavenProject\\Library\\driver\\chromedriver.exe");
				oBrowser=new ChromeDriver();
			}catch(Exception e)
			{
				e.printStackTrace();
			}
		}
		@Test(priority=2)
		private static void navigate()
		{

			try {
				String expected="actiTIME Inc.";
				oBrowser.get("http://localhost/login.do");
				Thread.sleep(5000);
				String actual=oBrowser.findElement(By.linkText("actiTIME Inc.")).getText();
				Assert.assertEquals(actual, expected);
			}catch(Exception e)
			{
				e.printStackTrace();
			}

		}
		@Test(priority=3,dataProvider ="login")
		private static void login(String name,String password)
		{
			try
			{
				String expected="keepLoggedInLabel";
				oBrowser.findElement(By.id("username")).sendKeys(name);
				oBrowser.findElement(By.name("pwd")).sendKeys(password);
				Thread.sleep(2000);
				/*String actual=oBrowser.findElement(By.linkText("keepLoggedInLabel")).getText();
				Assert.assertEquals(actual, expected);*/
				oBrowser.findElement(By.xpath("//*[@id='loginButton']/div")).click();
				Thread.sleep(5000);

			}catch(Exception e)
			{
				e.printStackTrace();
			}
		}
		@Test(priority=4)
		private static void minimize()
		{
			try
			{

				oBrowser.findElement(By.className("gettingStartedShortcutsLabel")).click();
				Thread.sleep(5000);
			}catch(Exception e)
			{
				e.printStackTrace();
			}
		}

		@Test(priority=5,dataProvider ="Createuser")
		private static void CreateUser(String firstname,String middlename,String lastname,String email,String user,String PWD,String rePWD)
		{
			try
			{
			
				oBrowser.findElement(By.xpath("//*[@id=\"topnav\"]/tbody/tr[1]/td[5]/a/div[2]")).click();
				Thread.sleep(2000);
				oBrowser.findElement(By.xpath("//div[text()='Add User']")).click();
				Thread.sleep(2000);
				oBrowser.findElement(By.name("firstName")).sendKeys(firstname);
				oBrowser.findElement(By.name("middleName")).sendKeys(middlename);
				oBrowser.findElement(By.name("lastName")).sendKeys(lastname);
				oBrowser.findElement(By.name("email")).sendKeys(email);
				oBrowser.findElement(By.name("username")).sendKeys(user);
				oBrowser.findElement(By.name("password")).sendKeys(PWD);
				oBrowser.findElement(By.name("passwordCopy")).sendKeys(rePWD);
				Thread.sleep(3000);
				oBrowser.findElement(By.xpath("//span[text()='Create User']")).click();
				Thread.sleep(2000);
			}catch(Exception e)
			{
				e.printStackTrace();
			}
		}
		@Test(priority=6,dataProvider = "modifyuser")
		private static void ModifyUser(String pass,String repass)
		{
			try
			{
				oBrowser.findElement(By.xpath("//*[@id=\'userListTableContainer\']/table/tbody/tr[1]/td[1]/div/table/tbody/tr/td/div[1]/span[2]")).click();
				Thread.sleep(3000);
				oBrowser.findElement(By.name("password")).sendKeys(pass);
				oBrowser.findElement(By.name("passwordCopy")).sendKeys(repass);
				Thread.sleep(2000);
				oBrowser.findElement(By.xpath("//*[@id=\'userDataLightBox_commitBtn\']/div/span")).click();
				Thread.sleep(2000);


			}catch(Exception e)
			{
				e.printStackTrace();
			}

		}
		@Test(priority=7)
		private static void DeleteUser()
		{
			String expected,actual;
			try

			{
				expected="Delete user";
				oBrowser.findElement(By.xpath("//*[@id=\"userListTableContainer\"]/table/tbody/tr[2]/td[1]/table/tbody/tr/td/div[1]")).click();
				Thread.sleep(3000);
				oBrowser.findElement(By.id("userDataLightBox_deleteBtn")).click();
				Thread.sleep(2000);
				actual=oBrowser.getTitle();
				assertEquals(expected, actual);
				Alert oAlert=oBrowser.switchTo().alert();
				//String content=oAlert.getText();
				//System.out.println(content);
				oAlert.accept();
				Thread.sleep(2000);

			}catch(Exception e)
			{
				e.printStackTrace();
			}
		}

		@Test(priority=8)
		private static void Logout()
		{
			String expected,actual;
			try 
			{
				expected="actiTIME - Login";
				oBrowser.findElement(By.linkText("Logout")).click();
				Thread.sleep(2000);
				actual=oBrowser.getTitle();
				assertEquals(expected,actual);

			}catch(Exception e)
			{
				e.printStackTrace();
			}

		}
		@Test(priority=9)
		private static void CloseApplication()
		{
			oBrowser.quit();
		}
		@DataProvider(name="login")
		public Object[][] getLoginData()
		{
			return new Object[][] {{"admin","manager"}};
		}
		@DataProvider(name="Createuser")
		public Object[][] createuser()
		{
			return new Object[][] {{"Rajendra","Rajesh","M","rajendra@gmail","santhu","ganu123","ganu123"}};

		}
		@DataProvider(name="modifyuser")
		public Object[][] modify()
		{
			return new Object[][] {{"raj120","paku123"}};

		}

	}




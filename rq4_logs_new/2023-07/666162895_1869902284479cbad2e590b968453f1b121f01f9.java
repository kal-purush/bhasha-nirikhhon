package Experiments_Main;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import com.aventstack.extentreports.ExtentReports;
import com.aventstack.extentreports.reporter.ExtentSparkReporter;
import com.aventstack.extentreports.reporter.configuration.Theme;

/*
		ExtentReports is Pre-defined Class, in Extent_Report Library.
		
		There are Multiple Type of ExtentReports in Reporters  ===>>>  ExtentSparkReporter (we will Used -- free),  ExtentAventReporter,  ExtentEmailReporter,  ExtentKlovReporter
 
 		 ExtentSparkReporter_Class,  to create report Configuration,  ( Theme, ReportName,  DocumentTilte, TimeStamp) in Report.
 
		Extent_Report is in HTML Format only.
	
		System.getProperty("user.dir")    ---> Project_Path
		
		 \\test-output\\ExtentReports\\extentReport.html     ---->   File_Path of Extent_Report 
		 
		 \\src\\main\\java\\com\\TutorialsNinja\\Qa\\Config\\config.properties"    ---> File_Path of Config_Properties_File
		 
		 Attach this class into Listner_Class to generate report at onStart() Method
		 
		 make static method for generateExtentReport() method, so we can assess without Creating Object of Class

 */

public class Extent_Reporter2 
{

			public static  ExtentReports generateExtentReport()                                           // Create Method to Generate_Report   ---> generateExtentReport() Method,  Return of Method is ExtentReport_Type
	
			{
						ExtentReports extentReport = new ExtentReports();                                          // Create Object of ExtentReport_Class --
					

						
						File extent_Report_File = new  File (System.getProperty("user.dir") + " \\test-output\\ExtentReports\\extentReport.html ") ;
						
						ExtentSparkReporter sparkReporter = new ExtentSparkReporter( extent_Report_File ) ;                                                                            // Create object of ExtentSparkReporter_class
						
																																															//  Using sparkReporter_Object,   set Configuration Like, Theme, ReportName, DocumentTitle,  TimeStamp
						
						sparkReporter.config().setTheme(Theme.DARK);                                                                       				 // Set DARK Theme of Report
						
						sparkReporter.config().setReportName(" Tutorials Ninja Test Automation Result Report ");        			// Set Report_Name
						
						sparkReporter.config().setDocumentTitle(" TN Automation Report");                                            		    // Set Document Tiltle
						
						sparkReporter.config().setTimeStampFormat("dd/MM/yyyy   hh:mm:ss ");                                              // Set TimeStamp
						
						
						
						extentReport.attachReporter(sparkReporter);                                                         					        // Attach SparkReporter, with extentReport.
				
																																																// Using Extent_Report Object, set System Information  like URL, UserName, PassWord,  BrowserName,  
						
						// Get URL, UserName, Password from Config.properties File
						
						Properties configProp = new Properties() ;                                          												    // Create Object of Properties_Class, for configProperty_File
						
						File config_Prop_File = new File ( System.getProperty("user.dir") + "\\src\\main\\java\\com\\TutorialsNinja\\Qa\\Config\\config.properties") ;       // Create Object of File_Class, to read Config.Properties_File
						
						try 
						{
						
						FileInputStream fisConfigProp = new FileInputStream ( config_Prop_File);                                        // Create Object of FileInputStream_Class, to read streams of characters, consider using FileReader, from Config.Properties_File
						
						configProp.load(fisConfigProp);
			
						}
						catch (Throwable e)
						{
							e.printStackTrace();
							System.out.println(e);
						}
						
																																		// Using Extent_Report Object, set System Information  like URL, UserName, PassWord,  BrowserName,  
						
						
						extentReport.setSystemInfo("Application URL : ", configProp.getProperty("url"));                                       //  setSystemInfo() Method --  Adds any applicable system information to all started reporters 
						
						extentReport.setSystemInfo("Browser Name: ",  configProp.getProperty("browserName"));
						
						extentReport.setSystemInfo("Email: ", configProp.getProperty("valid_Email"));
						
						extentReport.setSystemInfo("Password: ", configProp.getProperty("valid_Password"));
						
						extentReport.setSystemInfo("Operating System: ", System.getProperty("os.name"));  
						
						extentReport.setSystemInfo("Java Version: ", System.getProperty("java.version"));
						
						extentReport.setSystemInfo("UserName: ", System.getProperty("user.name"));
						
						
		
						return extentReport ;
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			}
	
	
	
	
	
	
	
	
	
}
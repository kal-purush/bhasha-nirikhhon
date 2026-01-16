package com.iiitb.ooadvoid;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class AccessProperties
{
	private String serverURL;
	private String serverPath;

	public String getServerURL()
	{
		return serverURL;
	}

	public void setServerURL(String serverURL)
	{
		this.serverURL = serverURL;
	}

	public String getServerPath()
	{
		return serverPath;
	}

	public void setServerPath(String serverPath)
	{
		this.serverPath = serverPath;
	}

//	public static void main(String[] args)
	AccessProperties()
	{

		Properties prop = new Properties();
		InputStream input = null;

		try
		{

			input = new FileInputStream("config.properties");

			// load a properties file 
			prop.load(input);

			// get the property value and print it out
//			System.out.println(prop.getProperty("serverURL"));
//			System.out.println(prop.getProperty("serverPath"));
			this.setServerURL(prop.getProperty("serverURL"));
			this.setServerPath(prop.getProperty("serverPath"));

		}
		catch (IOException ex)
		{
			ex.printStackTrace();
		}
		finally
		{
			if (input != null)
			{
				try
				{
					input.close();
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
			}
		}

	}

}
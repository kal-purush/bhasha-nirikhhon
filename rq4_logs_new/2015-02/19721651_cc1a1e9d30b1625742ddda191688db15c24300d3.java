package com.contactmanager.dao;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.yaml.snakeyaml.Yaml;

import com.contactmanager.constants.CMConstants;

/**
 * Created by sujay-2613 on 2/8/15.
 */
public class StartService
{
	private static Logger LOGGER = Logger.getLogger(StartService.class.getName());
	private static String appPath = CMConstants.APP_PATH;
	private static Map<String, String> schemaDefn = new HashMap<String, String>();

	public static void main(String args[])
	{
		Connection connection;
		Statement statement = null;
		try
		{
			connection = ContactManagerDaoFactory.createconnection();
			statement = connection.createStatement();
		}
		catch (Exception e)
		{
			LOGGER.log(Level.SEVERE, "Exception while getting database connection" + e);
		}
		schemaDefn = buildSchema();
		try
		{
			Iterator<Map.Entry<String, String>> itr = schemaDefn.entrySet().iterator();
			while (itr.hasNext())
			{
				String tableName = itr.next().getKey();
				LOGGER.log(Level.INFO, "Executing table creation for" + tableName);
				String query = schemaDefn.get(tableName);
				statement.execute(query);
			}
		}
		catch (Exception e)
		{
			LOGGER.log(Level.SEVERE, "Exception while getting table creation" + e);
		}
	}

	private static Map<String, String> buildSchema()
	{
		File schemaFile = new File(appPath + "/../conf/SchemaDefinition.yml");
		LOGGER.log(Level.INFO, "Schema File:" + schemaFile.toString());
		try
		{
			if (schemaDefn.isEmpty())
			{
				schemaDefn = (Map<String, String>) new Yaml().load(new FileInputStream(schemaFile));
				Collections.unmodifiableMap(schemaDefn);
			}
		}
		catch (Exception e)
		{
			LOGGER.log(Level.SEVERE, "Exception while getting schema definition file" + e);
		}
		return schemaDefn;
	}
}
package com.yukthi.tools.db;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

import com.yukthi.tools.db.data.migration.DataMigrationManager;
import com.yukthi.tools.db.data.migration.IDataSource;
import com.yukthi.tools.db.data.migration.IDataStorage;

/**
 * The Class DataMigration is the entry point for the application. This class
 * loads the properties file having source and storage details then passes the
 * source and storage to the data migration manager for migrating the data.
 * 
 * @author Pritam
 */
public class DataMigration
{

	/**
	 * The main method.
	 *
	 * @param args
	 *            the arguments
	 * @throws FileNotFoundException
	 *             the file not found exception
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 * @throws ClassNotFoundException
	 *             the class not found exception
	 * @throws NoSuchMethodException
	 *             the no such method exception
	 * @throws SecurityException
	 *             the security exception
	 * @throws InstantiationException
	 *             the instantiation exception
	 * @throws IllegalAccessException
	 *             the illegal access exception
	 * @throws IllegalArgumentException
	 *             the illegal argument exception
	 * @throws InvocationTargetException
	 *             the invocation target exception
	 */
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws FileNotFoundException, IOException, ClassNotFoundException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException
	{
		// Load the source properties file
		Properties propertiesSource = new Properties();
		propertiesSource.load(new FileInputStream(args[0]));
		String sourceType = propertiesSource.getProperty("source.type");
		
		if(sourceType == null)
		{
			throw new IllegalArgumentException("Please follow the proper format: 'source.type = data' in properties file");
		}
		
		Class<IDataSource> clsSource = (Class<IDataSource>) Class.forName(sourceType);
		Constructor<IDataSource> consSource = clsSource.getConstructor(Properties.class);
		IDataSource iDataSource = consSource.newInstance(propertiesSource);

		// Load the storage properties file
		Properties propertiesStorage = new Properties();
		propertiesStorage.load(new FileInputStream(args[1]));
		String storageType = propertiesStorage.getProperty("storage.type");
		
		if(storageType == null)
		{
			throw new IllegalArgumentException("Please follow the proper format: 'storage.type = data' in properties file");
		}
		
		Class<IDataStorage> clsStorage = (Class<IDataStorage>) Class.forName(storageType);
		Constructor<IDataStorage> consStorage = clsStorage.getConstructor(Properties.class);
		IDataStorage iDataStorage = consStorage.newInstance(propertiesStorage);

		new DataMigrationManager().migrate(iDataSource, iDataStorage);
	}
}
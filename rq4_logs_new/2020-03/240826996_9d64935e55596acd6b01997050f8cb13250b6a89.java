package model;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class DataStore implements Serializable {

	public University university; 
	
	
	
	public int myMethod() {
		
		return 3;
	}
	

	/**
	 * serialization and deserialization code adapted from 
	 * https://www.geeksforgeeks.org/object-graph-java-serialization/
	 */
	public boolean serialize() {
		try {
			FileOutputStream fos = new FileOutputStream("serialized.database"); 
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			
			oos.writeObject(this);
		}
		catch(Exception e) {
			return false;
		}
		
		return true;
	}
	
	/**
	 * serialization and deserialization code adapted from 
	 * https://www.geeksforgeeks.org/object-graph-java-serialization/
	 */	
	public static DataStore load()  throws Exception {
		FileInputStream fis = new FileInputStream("serialized.database"); 
		ObjectInputStream ois = new ObjectInputStream(fis);
		
		DataStore db = (DataStore) ois.readObject();
		
		return db;
	}
}
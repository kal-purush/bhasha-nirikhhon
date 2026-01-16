package com.chris.oscjp.chapter6;
import java.io.*;

import com.chris.oscjp.chapter5.Animal;
import com.chris.oscjp.chapter5.Cat;

public class TestSerial implements Serializable{

	int objInt = 999;
	char objChar = 'c';
	String objString = "Object";
	Animal ani;
	Cat cat;
	
	public static void main(String[] args){
		TestSerial ts = new TestSerial();
		ts.cat = new Cat("Tom");
		ts.ani = new Animal();
		
		try{
		FileOutputStream fos = new FileOutputStream("temp.ser");
		ObjectOutputStream oos = new ObjectOutputStream(fos);
		oos.writeObject(ts);
		} catch (IOException ioe) {
		ioe.printStackTrace();
		}
		try{
		FileInputStream fis = new FileInputStream("temp.ser ");
		ObjectInputStream ois = new ObjectInputStream(fis);
		ts = (TestSerial) ois.readObject();
		System.out.println("Restored ts cat name = " + ts.cat.name);
		ois.close();
		} catch (FileNotFoundException fnfe){
			fnfe.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} catch (ClassNotFoundException cnf) {
			cnf.printStackTrace();
		}
	}

}
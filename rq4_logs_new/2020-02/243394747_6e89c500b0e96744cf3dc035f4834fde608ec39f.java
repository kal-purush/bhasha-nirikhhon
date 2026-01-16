package com.demo.Json;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        try{
        	ObjectMapper mapper = new ObjectMapper();
        	TypeReference<Demo> ref = new TypeReference<Demo>() {};
        	InputStream in = new FileInputStream("/home/parallels/Documents/docs/demo.json");
        	Demo demo = mapper.readValue(in, ref);
        	System.out.println( "Person name:"+demo.getPerson().getName() );
        	List<Car> cars = demo.getPerson().getCars().getCar();
        	for(Car c : cars) {
        		System.out.println("Car brand is "+c.getBrand());
        	}
        }
        catch(Exception e) {
        	e.printStackTrace();
        }
    }
}
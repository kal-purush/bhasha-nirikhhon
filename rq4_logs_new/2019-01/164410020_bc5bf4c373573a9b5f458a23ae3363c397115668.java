package com.application;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class DataFileProcessorTests {

    private static final String TAG = "com.application.DataFileProcessorTests";
    private static String parameter_1 = "ID";
    private static String parameter_2 = "54808168L";//Shelly Pane's ID
    private static  String dataFile = "src/test/test_input.txt";

    private static DataFileProcessor dataFileProcessor;


    /*
    *   Initialize with parameters, called twice for each test
     */
    @BeforeClass
    public static void init(){
        System.out.println("Initializing ID parameters");
        dataFileProcessor = DataFileProcessor.setProcessingParameters(dataFile, parameter_1, parameter_2).processDataFile();
    }

    /*
    *   Test if the size of the set is the appropriate size
     */
    @Test
    public void testIdSize(){

        System.out.println("Testing ID parameters");
        if(parameter_1.equals("ID"))
            assertEquals("Checking number of cities after processing Datafile", 4, dataFileProcessor.citiesIdVisited_Size());
    }

    /*
    * Test if the size of the map is the appropriate size
     */
    @Test
    public void testCitySize(){
        System.out.println("Testing City parameters");
        if(parameter_1.equals("CITY"))
            assertEquals("Checking number of individuals who visited city after processing Datafile", 10, dataFileProcessor.idVisitedCities_Size());

    }

    /*
     *   Change parameters to test for city parameter
     */
    @After
    public void testProcessorAfterParameterChange(){
        System.out.println("Initializing City parameters");
        parameter_1 = "CITY";
        parameter_2 = "BARCELONA";
        dataFileProcessor = DataFileProcessor.setProcessingParameters(dataFile, parameter_1, parameter_2).processDataFile();
    }



}
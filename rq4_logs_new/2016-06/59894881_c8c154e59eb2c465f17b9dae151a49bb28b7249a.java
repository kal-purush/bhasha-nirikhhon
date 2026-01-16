/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import converter.consoleedition.Temperature;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author James
 */
public class TemperatureTest
{
    
    public TemperatureTest()
    {
    }
    
    @BeforeClass
    public static void setUpClass()
    {
    }
    
    @AfterClass
    public static void tearDownClass()
    {
    }
    
    @Before
    public void setUp()
    {
    }
    
    @After
    public void tearDown()
    {
    }
    
    @Test
    public void FToFTest()
    {
        //testing to see if Farenheit to farenheit works
        Temperature TestFToF = new Temperature();
        TestFToF.conversionValue = 23;
        Assert.assertEquals("23 degrees farenheit should be 23 degrees farenheit", 23, TestFToF.conFromFarenheitToFarenheit(), 1);
    }
    
    @Test
    public void FToCTest()
    {
        //testing to see if farenheit to celsius works
        Temperature TestFToC = new Temperature();
        TestFToC.conversionValue = 87;
        Assert.assertEquals("87 degrees farenheit should be 31 degrees celsius", 31, TestFToC.conFromFarenheitToCelsius(), 1);
    }
    
    @Test
    public void CToFTest()
    {
        //testing to see if celsius to farenhet works properly
        Temperature TestCToF = new Temperature();
        TestCToF.conversionValue = 39;
        Assert.assertEquals("39 degrees celcius should be 102 degrees farenheit", 102, TestCToF.conFromCelsiusToFarenheit(), 1);
    }
    
    @Test
    public void CToCTest()
    {
        //testing to see if celsius to celsius works
        Temperature TestCToC = new Temperature();
        TestCToC.conversionValue = 67;
        Assert.assertEquals("67 degrees celcius is 67 degrees celcius", 67, TestCToC.conFromCelsiusToCelsius(), 1);
    }
    

    // TODO add test methods here.
    // The methods must be annotated with annotation @Test. For example:
    //
    // @Test
    // public void hello() {}
}
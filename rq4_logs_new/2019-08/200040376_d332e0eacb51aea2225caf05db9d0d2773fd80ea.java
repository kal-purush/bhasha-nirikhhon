package com.rezgateway.automation.tourmapper;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.testng.Assert;
import org.testng.ITestResult;
import org.testng.Reporter;
import org.testng.annotations.Parameters;

import com.rezgateway.automation.JavaHttpHandler;
import com.rezgateway.automation.builder.request.AvailabilityRequestBuilder;
import com.rezgateway.automation.enu.ConfirmationType;
import com.rezgateway.automation.pojo.AvailabilityRequest;
import com.rezgateway.automation.pojo.AvailabilityResponse;
import com.rezgateway.automation.pojo.DailyRates;
import com.rezgateway.automation.pojo.Hotel;
import com.rezgateway.automation.pojo.HttpResponse;
import com.rezgateway.automation.pojo.RateplansInfo;
import com.rezgateway.automation.pojo.Room;
import com.rezgateway.automation.reader.response.AvailabilityResponseReader;
import com.rezgateway.automation.reports.ExtentTestNGReportBuilderExt;
import com.rezgateway.automation.xmlout.utill.DataLoader;
import com.rezgateway.automation.xmlout.utill.ExcelDataSingleton;

public class AV_SC_ID58_TEST extends ExtentTestNGReportBuilderExt{

	AvailabilityResponse AvailabilityResponse = new AvailabilityResponse();
	AvailabilityRequest AviRequest = new AvailabilityRequest();

	@Parameters("TestUrl")
	@Test(priority = 0)
	public synchronized void availbilityTest(String TestUrl) throws Exception {

		getAvailabilityData();
		
		String Scenario     = AviRequest.getScenarioID();
		String HotelCode    = Arrays.toString(AviRequest.getCode());
		String SearchString = AviRequest.getCheckin()+"|"+AviRequest.getCheckout()+" |"+AviRequest.getNoOfRooms()+"R| "+AviRequest.getUserName()+" |"+AviRequest.getPassword();
				
		ITestResult result = Reporter.getCurrentTestResult();
		String testname = "Test Scenario:" + Scenario + " : Search By : " + AviRequest.getSearchType() + " Code : " + HotelCode+ "Criteria : "+SearchString;
		result.setAttribute("TestName", testname);
		result.setAttribute("Expected", "Results Should be available");

		JavaHttpHandler handler = new JavaHttpHandler();
		HttpResponse Response = handler.sendPOST(TestUrl, "xml=" + new AvailabilityRequestBuilder().buildRequest("Resources/RegressionSuite_AvailRequest_senarioID_" + AviRequest.getScenarioID() + ".xml", AviRequest));

		if (Response.getRESPONSE_CODE() == 200) {

			AvailabilityResponse = new AvailabilityResponseReader().getResponse(Response.getRESPONSE());

			if (AvailabilityResponse.getHotelCount() > 0) {
				result.setAttribute("Actual", "Results Available, Hotel Count :" + AvailabilityResponse.getHotelCount());
			} else {
				result.setAttribute("Actual", "Results not available Error Code :" + AvailabilityResponse.getErrorCode() + " Error Desc :" + AvailabilityResponse.getErrorDescription());
				Assert.fail("No Results Error Code :" + AvailabilityResponse.getErrorCode());
			}
		} else {
			result.setAttribute("Actual", "No Response recieved Code :" + Response.getRESPONSE_CODE());
			Assert.fail("Invalid Response Code :" + Response.getRESPONSE_CODE() + " ,No Response received");
		}
	}

	@Test(dependsOnMethods = "availbilityTest")
	public synchronized void isAvailabileHotelName() {

		ITestResult result = Reporter.getCurrentTestResult();
		result.setAttribute("TestName", "Testing Availability of the Hotel name in the response ");
		result.setAttribute("Expected", "Hotel Hotel name Should be Available in the Response ");

		if (!AvailabilityResponse.getHotelList().entrySet().iterator().next().getValue().getName().isEmpty()) {
			result.setAttribute("Actual", "Hotel name is  : " + AvailabilityResponse.getHotelList().entrySet().iterator().next().getValue().getName());
		} else {
			result.setAttribute("Actual", "Hotel Short Description is not Available in the Response : " + AvailabilityResponse.getHotelList().entrySet().iterator().next().getValue().getName());
			Assert.fail("Hotel name is not Available in the Response");
		}

	}

	
	
	  @Test(dependsOnMethods = "availbilityTest") 
	  public synchronized void testPromotionCode() {
	  
	  ITestResult result = Reporter.getCurrentTestResult();
	  result.setAttribute("TestName", "Testing the availability of promotion Code ");
	  result.setAttribute("Expected", "Promotion Code should be available");
	  ArrayList<String> flag = new ArrayList<String>();
	  
	  try {
           
			Hotel hotelInResponse = AvailabilityResponse.getHotelList().entrySet().iterator().next().getValue();
			Map<String, ArrayList<Room>> rooms = hotelInResponse.getRoomInfo();
			String promoCode = "";
			for(Room room : rooms.entrySet().iterator().next().getValue() ){
				
				promoCode = room.getPromotionCode();
				System.out.print("-----------promoCode : " + promoCode);
				
				if("".equals(promoCode)){
					System.out.print("-----------promoCode : Blank");
					flag.add("NOPROMO");

				}
				
			}
			if (flag.contains("NOPROMO")) {
				result.setAttribute("Actual", "Promo code is not availble for Some rooms");
				Assert.fail("Promo code is not availble for Some rooms");
			} else {
				result.setAttribute("Actual", "Promo code is availble for All the rooms in the Response. PromoCode is : " + promoCode);
			}
			
			

		} catch (Exception e) {
			System.out.println(e);
			result.setAttribute("Actual", e);
			Assert.fail("This testDayWiseRateAvailability is Failed due to :", e);
		}

	}
	 
	@Test(dependsOnMethods = "availbilityTest")
	public synchronized void testDayWiseRateAvailability() {

		ITestResult result = Reporter.getCurrentTestResult();
		result.setAttribute("TestName", "Testing Daily Rates having for Each date in the Response ");
		result.setAttribute("Expected", "System shouls contain the  Daily Rate for each day");
		try {

			Hotel hotelInResponse = AvailabilityResponse.getHotelList().entrySet().iterator().next().getValue();
			Map<String, ArrayList<Room>> rooms = hotelInResponse.getRoomInfo();
			TreeMap<String, DailyRates> dailyRates = new TreeMap<String, DailyRates>();
			ArrayList<String> flag = new ArrayList<String>();

			for (Room room : rooms.entrySet().iterator().next().getValue()) {

				Map<String, RateplansInfo> RatesPlanInfos = room.getRatesPlanInfo();
				dailyRates = RatesPlanInfos.entrySet().iterator().next().getValue().getDailyRates();

				if (Integer.parseInt(AviRequest.getNoofNights()) == dailyRates.size()) {
					flag.add("true");
				} else {
					flag.add("false");
				}
			}

			if (flag.contains("false")) {
				result.setAttribute("Actual", "Daily rates are not availble for Some room");
				Assert.fail("Daily rates are not availble for Some room");
			} else {
				result.setAttribute("Actual", "Daily rates are availble for All the rooms in the Response ");
			}

		} catch (Exception e) {
			System.out.println(e);
			result.setAttribute("Actual", e);
			Assert.fail("This testDayWiseRateAvailability is Failed due to :", e);
		}

	}
	
	@Test(dependsOnMethods = "availbilityTest")
	public synchronized void testFreeNightAvailability() {

		ITestResult result = Reporter.getCurrentTestResult();
		result.setAttribute("TestName", "Testing the availability of Free night in the Response ");
		result.setAttribute("Expected", "Respone should contain a free night");
		ArrayList<String> flag = new ArrayList<String>();
		int roomCount = 0;
		try {

			Hotel hotelInResponse = AvailabilityResponse.getHotelList().entrySet().iterator().next().getValue();
			
			//why we use a map?
			Map<String, ArrayList<Room>> rooms = hotelInResponse.getRoomInfo();
			TreeMap<String, DailyRates> dailyRates = new TreeMap<String, DailyRates>();
			
			//room count is the size of the array inside the map
			roomCount = rooms.entrySet().iterator().next().getValue().size();
			
			//To get the room objects in arraylist
			for (Room room : rooms.entrySet().iterator().next().getValue()) {
				Map<String, RateplansInfo> RatesPlanInfos = room.getRatesPlanInfo();
				
				//get the daily rates tree map
				dailyRates = RatesPlanInfos.entrySet().iterator().next().getValue().getDailyRates();
					
				for(Map.Entry<String,DailyRates> dailyRate : dailyRates.entrySet()) {
					DailyRates dailyRatesObject = dailyRate.getValue();
					double total = dailyRatesObject.getTotal();
									
					if(total == 0) {
						flag.add("FN");
					}
					
				}
			}

						
			if (flag.size() == roomCount) {
				result.setAttribute("Actual", "Free Night promotion is availble for all rooms");
			} else {
				result.setAttribute("Actual", "Free Night promotion is not availble for all rooms");
				Assert.fail("Fail : Free Night promotion is not availble for all rooms");
			}

		} catch (Exception e) {
			System.out.println(e);
			result.setAttribute("Actual", e);
			Assert.fail("This testFreeNightAvailability is Failed due to :", e);
		}

	}
	
	
	public AvailabilityRequest getAvailabilityData() throws Exception {

		DataLoader loader = new DataLoader();
		AviRequest = loader.getReservationReqObjList(ExcelDataSingleton.getInstance("Resources/Full Regression Testing Checklist.xls", "Scenario").getDataHolder())[57][0];
		return AviRequest;

	}

}

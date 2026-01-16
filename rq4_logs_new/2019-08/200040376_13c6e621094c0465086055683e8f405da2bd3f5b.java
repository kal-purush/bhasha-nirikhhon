package com.rezgateway.automation.tourmapper;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.testng.Assert;
import org.testng.ITestResult;
import org.testng.Reporter;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.rezgateway.automation.JavaHttpHandler;
import com.rezgateway.automation.builder.request.AvailabilityRequestBuilder;
import com.rezgateway.automation.builder.request.CancellationRequestBuilder;
import com.rezgateway.automation.builder.request.ReservationRequestBuilder;
import com.rezgateway.automation.enu.ConfirmationType;
import com.rezgateway.automation.pojo.AvailabilityResponse;
import com.rezgateway.automation.pojo.BookingPolicy;
import com.rezgateway.automation.pojo.CancellationRequest;
import com.rezgateway.automation.pojo.CancellationResponse;
import com.rezgateway.automation.pojo.DailyRates;
import com.rezgateway.automation.pojo.Hotel;
import com.rezgateway.automation.pojo.HttpResponse;
import com.rezgateway.automation.pojo.RateplansInfo;
import com.rezgateway.automation.pojo.ReservationRequest;
import com.rezgateway.automation.pojo.ReservationResponse;
import com.rezgateway.automation.pojo.Room;
import com.rezgateway.automation.reader.response.AvailabilityResponseReader;
import com.rezgateway.automation.reader.response.CancellationResponseReader;
import com.rezgateway.automation.reader.response.ReservationResponseReader;
import com.rezgateway.automation.reports.ExtentTestNGReportBuilderExt;
import com.rezgateway.automation.xmlout.utill.DataLoader;
import com.rezgateway.automation.xmlout.utill.ExcelDataSingleton;

////Cancellation scenario Confirmed ouside cancellation period

public class CNX_SC_ID02_TEST extends ExtentTestNGReportBuilderExt {
	
	AvailabilityResponse AvailabilityResponse = null;
	ReservationRequest ResRequest = null;
	ReservationResponse ResResponse = null;
	CancellationResponse cnxR = null;

	@Parameters({ "TestUrlRes", "TestUrl", "TestUrlCnx" })
	@Test(priority = 0)
	public synchronized void cancellationTesting(String TestUrlRes, String TestUrl, String TestUrlCnx) throws Exception {

		getAvailabilityData();
		
		String Scenario     = ResRequest.getScenarioID();
		String HotelCode    = Arrays.toString(ResRequest.getCode());
		
		//Within Cancellation period less than 10
		  DateTimeFormatter dtf = DateTimeFormatter.ofPattern("dd-MMM-yyyy");
		   LocalDateTime now = LocalDateTime.now();
		   now= now.plusDays(90);//outside cancel >10
		   String newcheckin=dtf.format(now);
		   now= now.plusDays(2);
		   String newcheckout=dtf.format(now);
		   
		   ResRequest.setCheckin(newcheckin);
		   ResRequest.setCheckout(newcheckout);
		   
		   
		String SearchString = ResRequest.getCheckin()+"|"+ResRequest.getCheckout()+" |"+ResRequest.getNoOfRooms()+"R| "+ResRequest.getUserName()+" |"+ResRequest.getPassword();
		
		ITestResult results = Reporter.getCurrentTestResult();
		String testname = "Test Scenario:" + Scenario + " : Search By : " + ResRequest.getSearchType() + " Code : " + HotelCode+ "Criteria : "+SearchString;
		
		results.setAttribute("TestName", testname);
		results.setAttribute("Expected", "System should be booked this Hotel");
		// http://192.168.1.62:8380/bonotelapps/bonotel/reservation/GetReservation.do

		// === Availability Check Started === /

		JavaHttpHandler handler = new JavaHttpHandler();
		HttpResponse Av_Response = handler.sendPOST(TestUrl, "xml=" + new AvailabilityRequestBuilder().buildRequest("Resources/RegessionSuite_AvailRequest_senarioID_" + ResRequest.getScenarioID() + ".xml", ResRequest));

		if (Av_Response.getRESPONSE_CODE() == 200) {
			AvailabilityResponse = new AvailabilityResponseReader().getResponse(Av_Response.getRESPONSE());
			if (AvailabilityResponse.getHotelCount() > 0) {
				results.setAttribute("Actual", "Results Available, Hotel Count :" + AvailabilityResponse.getHotelCount());
				// === Reservation Request Data Initiated === //
				Hotel hotel = AvailabilityResponse.getHotelList().entrySet().iterator().next().getValue();
				ResRequest.setCurrency(hotel.getRateCurrencyCode());

				Iterator<Entry<String, ArrayList<Room>>> itr = hotel.getRoomInfo().entrySet().iterator();

				Double TotalRate = 0.00;
				Double TotalTax = 0.00;
				int i = 0;
				while (itr.hasNext()) {
					Map.Entry<String, ArrayList<Room>> entry = (Map.Entry<String, ArrayList<Room>>) itr.next();
					// String roomnum = entry.getKey().trim();

					Room room = entry.getValue().get(0);
					room.setAdultsCount(ResRequest.getRoomlist().get(i).getAdultsCount());
					room.setChildCount(ResRequest.getRoomlist().get(i).getChildCount());
					room.setChildAges(ResRequest.getRoomlist().get(i).getChildAges());
					room.setConType(ResRequest.getRoomlist().get(i).getConType());
					Entry<String, RateplansInfo> plan = room.getRatesPlanInfo().entrySet().iterator().next();
					RateplansInfo planinfo = plan.getValue();
					room.setRatePlanCode(planinfo.getRatePlanCode());
					Double atax = planinfo.getTaxInfor().get("roomTax").getTaxAmount();
					Double btax = planinfo.getTaxInfor().get("salesTax").getTaxAmount();
					Double ctax = planinfo.getTaxInfor().get("otherCharges").getTaxAmount();
					TotalRate += planinfo.getTotalRate();
					TotalTax += (atax + btax + ctax);
					i++;
					ResRequest.addToRezRoomList(room);
					ResRequest.setConfType((room.getConType()));
				}

				ResRequest.setTotal(Double.toString(TotalRate));
				ResRequest.setTotalTax(Double.toString(TotalTax));
				// === Reservation Request Data Initiated === //
			} else {
				results.setAttribute("Actual", "Results not available Error Code :" + AvailabilityResponse.getErrorCode() + " Error Desc : " + AvailabilityResponse.getErrorDescription());
				Assert.fail("No Results Error Code : " + AvailabilityResponse.getErrorCode());
			}

		} else {
			results.setAttribute("Actual", "No Response recieved Code :" + Av_Response.getRESPONSE_CODE());
			Assert.fail("Invalid Response Code :" + Av_Response.getRESPONSE_CODE() + " ,No Response received");
		}
		// === Availability Check End === /

		HttpResponse Response = handler.sendPOST(TestUrlRes, "xml=" + new ReservationRequestBuilder().buildRequest("Resources/RegressionSuite_ReservationRequest_senarioID_" + ResRequest.getScenarioID() + ".xml", ResRequest));

		if (Response.getRESPONSE_CODE() == 200) {
			ResResponse = new ReservationResponseReader().getResponse(Response.getRESPONSE());

			CancellationRequest cnx = new CancellationRequest();
			LocalDateTime date = LocalDateTime.now();
			DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

			cnx.setCancellationRequestTimestamp(date.format(formatter));
			cnx.setUserName(ResRequest.getUserName());
			cnx.setPassword(ResRequest.getPassword());
			cnx.setSupplierReferenceNo(ResResponse.getReferenceno());
			cnx.setCancellationNotes("Cancellation_note_senarioID__" + ResRequest.getScenarioID());
			cnx.setCancellationReason("Cancellation_Reason_senarioID__ " + ResRequest.getScenarioID());

			HttpResponse cnxResponse = handler.sendPOST(TestUrlCnx, "xml=" + new CancellationRequestBuilder().buildRequest("Resources/RegressionSuite_CancellationRequest_senarioID_" + ResRequest.getScenarioID() + ".xml", cnx));
			cnxR = new CancellationResponseReader().getResponse(cnxResponse.getRESPONSE());

			if (cnxResponse.getRESPONSE_CODE() == 200) {

				if (("Y").equals(cnxR.getCancellationResponseStatus())) {
					results.setAttribute("Actual", "Cancellation Done : " + ResResponse.getReferenceno() + " / " + cnxR.getCancellationNo() + " : " + cnxR.getCanellationFee());
				} else {
					results.setAttribute("Actual", "Cancellation is not done due to  :" + cnxR.getErrorCode() + " / " + cnxR.getErrorDescription());
					Assert.fail("Cancellation is not done due to  :" + cnxR.getErrorCode() + " / " + cnxR.getErrorDescription());
				}

			} else {
				results.setAttribute("Actual", "No Response recieved Code :" + cnxResponse.getRESPONSE_CODE());
				Assert.fail("Invalid Response Code : " + cnxResponse.getRESPONSE_CODE() + " / " + cnx.getCancellationReason() + " ,No Response received");
			}

		} else {
			results.setAttribute("Actual", "No Response recieved Code :" + Response.getRESPONSE_CODE());
			Assert.fail("Invalid Response Code :" + Response.getRESPONSE_CODE() + " ,No Response received");
		}

	}



	
	// currently this test doing for static data
	@Test(dependsOnMethods = "cancellationTesting" ,priority = 2)
	public synchronized void testCancellationPolicy() {

		ITestResult result = Reporter.getCurrentTestResult();
		result.setAttribute("TestName", "Testing is applying Cancellation policy");
		result.setAttribute("Expected", "System should display cnx policy in Checkin Checkout dates ");

		try {
			Hotel hotelInResponse = AvailabilityResponse.getHotelList().get(ResRequest.getCode()[0]);
			Map<String, ArrayList<Room>> rooms = hotelInResponse.getRoomInfo();
			ArrayList<String> flag = new ArrayList<String>();

			for (Room r : rooms.entrySet().iterator().next().getValue()) {

				
				if(r.getRoomPolicy()==null)
				{
					result.setAttribute("Actual", " policy not found ");
					flag.add("False");
					Assert.fail("Cancellation policy not found ");
				}
				
				for(BookingPolicy bockingPolicy : r.getRoomPolicy() ){
		
					if(("2019-02-17".equals(bockingPolicy.getPolicyFrom())&&("2021-02-17".equals(bockingPolicy.getPolicyTo())))?flag.add("PolicyFromAndTo_True") : flag.add("PolicyFromAndTo_False"));
					if("Cancel".equals(bockingPolicy.getAmendmentType())? flag.add("AmendmentType_True") : flag.add("AmendmentType_False"));
					if("percentage".equals(bockingPolicy.getPolicyBasedOn())? flag.add("PolicyBasedOn_True") : flag.add("PolicyBasedOn_False"));
					if("10".equals(bockingPolicy.getPolicyBasedOnValue())? flag.add("PolicyBasedOn_True"):flag.add("PolicyBasedOn_False"));						
					if(10==(bockingPolicy.getArrivalRangeValue()) ? (flag.add("getArrivalRangeValue_True")):(flag.add("getArrivalRange_False")));
					if("$92.4".equals(bockingPolicy.getPolicyFee()) ? flag.add("PolicyFee_True") : flag.add("PolicyFee_True"));
					if("$924.00".equals(bockingPolicy.getNoShowPolicyFee()) ? flag.add("NoShowPolicyFee_True") : flag.add("NoShowPolicyFee_False")  );
				}
				
			}
			
			String check = flag.toString();
			
			if (check.contains("False")) {
				result.setAttribute("Actual", " Cancellation policy is wrong ");
				Assert.fail("Cancellation policy is wrong ");
				System.out.println(check);
			} else {
				result.setAttribute("Actual", "Cancellation policy is Correct ");
			}

		} catch (Exception e) {
			System.out.println(e);
			result.setAttribute("Actual", e);
			Assert.fail("This testCancellationPolicy is Failed due to :", e);
		}

	}

	@Test(dependsOnMethods= "cancellationTesting")
	public synchronized void testCancellationFee(){
		
		ITestResult result= Reporter.getCurrentTestResult();
		result.setAttribute("TestName", "Testing the Cancellation Fee is correctly applying when do the Cancellation");
		result.setAttribute("Expected", "Cancellation fee Should be Applyed according to the CNX policy");
	
	try {
			if(("0").equals(cnxR.getCanellationFee())){
				result.setAttribute("Actual", " Standard Cancellation Fee is Correct ");
			}else{
				result.setAttribute("Actual", " Standard Cancellation Fee is inCorrect ");
				Assert.fail("Standard Cancellation Fee is inCorrect ");
			}
				
		} catch (Exception e) {
			System.out.println(e);
			result.setAttribute("Actual", e);
			Assert.fail("This testCancellationPolicy is Failed due to :", e);
		}
			

	}
	
	

	@Test(dependsOnMethods = "cancellationTesting",priority = 1)
	public synchronized void isHotelOnRequestedAviResponse() {

		ITestResult result = Reporter.getCurrentTestResult();
		result.setAttribute("TestName", "Testing the Hotel is on OnRequest status For this Checkin Checkout dates ");
		result.setAttribute("Expected", "Should not be on OnRequest status For this Checkin Checkout dates ");
		try {
			Hotel hotelInResponse = AvailabilityResponse.getHotelList().get(ResRequest.getCode()[0]);
			Map<String, ArrayList<Room>> rooms = hotelInResponse.getRoomInfo();
			ArrayList<String> flag = new ArrayList<String>();
			for (Room r : rooms.entrySet().iterator().next().getValue()) {
				if (ConfirmationType.CON == r.getConType()) {
					flag.add("CON");
				} else {
					flag.add("REQ");
				}
			}
			if (flag.contains("REQ")) {
				result.setAttribute("Actual", "Some Hotel rooms are On requested ");
				Assert.fail("Some Hotel rooms are On requested ");
			} else {
				result.setAttribute("Actual", "All hotel Rooms Confirmation status is CON ");
			}

		} catch (Exception e) {
			System.out.println(e);
			result.setAttribute("Actual", e);
			Assert.fail("This testNumOfRoomsAvailbility is Failed due to :", e);
		}
	}

	public ReservationRequest getAvailabilityData() throws Exception {

		DataLoader loader = new DataLoader();
		ResRequest = loader.getReservationReqObjList(ExcelDataSingleton.getInstance("Resources/Full Regression Testing Checklist.xls", "Scenario").getDataHolder())[66][0];
		return ResRequest;

	}

}
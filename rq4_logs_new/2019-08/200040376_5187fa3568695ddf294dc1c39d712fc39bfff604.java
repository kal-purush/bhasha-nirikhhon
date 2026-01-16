package com.rezgateway.automation.xmlout.utill;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;

import com.rezgateway.automation.pojo.AvailabilityRequest;
import com.rezgateway.automation.pojo.CancellationRequest;
import com.rezgateway.automation.pojo.ModificationRequest;
import com.rezgateway.automation.pojo.ReservationRequest;
import com.rezgateway.automation.pojo.Room;

public class DataLoader {

	public AvailabilityRequest[][] getAvailabilityObjList(String[][] AvailMatrix) {

		AvailabilityRequest[][] AvailList = new AvailabilityRequest[AvailMatrix.length][1];

		for (int i = 0; i < AvailMatrix.length; i++) {

			String[] availabilityReq = AvailMatrix[i];
			AvailabilityRequest Req = new AvailabilityRequest();

			Req.setAvailabitlyCancerlatioPolicy("Y");
			Req.setAvailabitlyHotelFee("Y");
			Req.setScenarioID(availabilityReq[0].trim());
			if (availabilityReq[1].trim().equalsIgnoreCase("HOTEL ID") || availabilityReq[1].trim().equalsIgnoreCase("HOTEL ID(10)") || availabilityReq[1].trim().equalsIgnoreCase("HOTEL ID(25)")) {
				Req.setSearchType("HOTELCODE");
				String[] hotelcodes = availabilityReq[2].trim().replace(".0", "").split(";");
				Req.setCode(hotelcodes);

			} else if (availabilityReq[1].trim().equalsIgnoreCase("CITY CODE")) {
				Req.setSearchType("CITYCODE");
				String[] codes = availabilityReq[2].trim().replace(".0", "").split(";");
				Req.setCode(codes);

			} else if (availabilityReq[1].trim().equalsIgnoreCase("HOTEL GROUP CODE")) {
				Req.setSearchType("HOTELGROUPCODE");
				String[] codes = availabilityReq[2].trim().replace(".0", "").split(";");
				Req.setCode(codes);
			} else if (availabilityReq[1].trim().equalsIgnoreCase("IATA CODE")) {
				Req.setSearchType("IATACODE");
				String[] codes = availabilityReq[2].trim().replace(".0", "").split(";");
				Req.setCode(codes);
			} else {
				System.out.println("Not Enterd Correctly Search type");
			}

			Req.setUserName(availabilityReq[8].trim());
			Req.setPassword(availabilityReq[9].trim());
			Req.setCheckin(availabilityReq[3].trim());
			Req.setCheckout(availabilityReq[4].trim());
			Req.setNoofNights(availabilityReq[5].trim().replace(".0", ""));
			Req.setNoOfRooms(availabilityReq[6].trim().replace(".0", ""));
			Req.setState(availabilityReq[10].trim());
			Req.setCountry(availabilityReq[11].trim());

			String[] RoomDetails = availabilityReq[7].trim().split(";");
			ArrayList<Room> roomList = new ArrayList<Room>();

			for (int j = 0; j < RoomDetails.length; j++) {

				Room r = new Room();

				if (RoomDetails[j].startsWith("[")) {
					// String code = "[{1,2,[5,6]};{1,3,[5,6,7]};{1,0,[]}]";
					r.setRoomTypeID("0");
					
					if(availabilityReq[15].isEmpty() || availabilityReq[15].split("\\,")[3].trim() == null || availabilityReq[15].split("\\,")[3].trim().isEmpty()){
						r.setBedTypeID("0");
					}else{
						r.setBedTypeID(availabilityReq[15].split("\\,")[3].trim());
					}
					
					r.setAdultsCount(RoomDetails[j].substring(2, 3).trim());
					r.setChildCount(RoomDetails[j].substring(4, 5).trim());

					if (!"0".equals(r.getChildCount()) || r.getChildCount().isEmpty()) {
						String[] childAges = RoomDetails[j].substring(7).split(",");
						childAges[childAges.length - 1] = childAges[childAges.length - 1].replace(childAges[childAges.length - 1], childAges[childAges.length - 1].substring(0, 1));
						r.setChildAges(childAges);
					}/* else {
						//System.out.println("Child Count is Zero");
					}*/
				} else {
					r.setRoomTypeID("0");
					
					if(availabilityReq[15].isEmpty() || availabilityReq[15].split("\\,")[3].trim() == null || availabilityReq[15].split("\\,")[3].trim().isEmpty()){
						r.setBedTypeID("0");
					}else{
						r.setBedTypeID(availabilityReq[15].split("\\,")[3].trim());
					}
					
					r.setAdultsCount(RoomDetails[j].substring(1, 2).trim());
					r.setChildCount(RoomDetails[j].substring(3, 4).trim());
					if (!"0".equals(r.getChildCount()) || r.getChildCount().isEmpty()) {

						String[] childAges = RoomDetails[j].substring(6).trim().split(",");
						childAges[childAges.length - 1] = childAges[childAges.length - 1].replace(childAges[childAges.length - 1], childAges[childAges.length - 1].substring(0, 1).trim());
						r.setChildAges(childAges);

					}/*else {
						//System.out.println("Child Count is Zero");
					}*/
				}
				roomList.add(r);
			}
			Req.setRoomlist(roomList);
			// Request building goes here
			AvailList[i][0] = Req;
		}

		return AvailList;

	}

	public ModificationRequest[][] getModObjList(String[][] ModMatrix) {

		ModificationRequest[][] ModList = new ModificationRequest[ModMatrix.length][1];

		for (int i = 0; i < ModMatrix.length; i++) {
			String[] ModificationReq = ModMatrix[i];

			ModificationRequest Req = new ModificationRequest();
			Req.setCheckin(ModificationReq[3]);
			;

			// Mod Request building goes here

			ModList[i][0] = Req;
		}

		return ModList;

	}

	public CancellationRequest[][] getCancelObjList(String[][] CanMatrix) {

		CancellationRequest[][] CanList = new CancellationRequest[CanMatrix.length][1];

		for (int i = 0; i < CanMatrix.length; i++) {
			String[] CancelReq = CanMatrix[i];

			CancellationRequest Req = new CancellationRequest();
			LocalDateTime date = LocalDateTime.now();
			DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
			Req.setSupplierReferenceNo(CancelReq[0]);
			Req.setUserName(CancelReq[1]);
			Req.setPassword(CancelReq[2]);
			Req.setCancellationReason(CancelReq[3]);
			Req.setCancellationRequestTimestamp(date.format(formatter));
			// Mod Request building goes here

			CanList[i][0] = Req;
		}

		return CanList;

	}

	public ReservationRequest[][] getReservationReqObjList(String[][] ResReqMatrix) {

		ReservationRequest[][] ResReqList = new ReservationRequest[ResReqMatrix.length][1];

		for (int i = 0; i < ResReqMatrix.length; i++) {
			String[] ResReq = ResReqMatrix[i];

			ReservationRequest Req = new ReservationRequest();
			Req.setScenarioID(ResReq[0].trim());
			// Req.setAvailabitlyCancerlatioPolicy("Y");
			// Req.setAvailabitlyHotelFee("Y");
			if (ResReq[1].trim().equalsIgnoreCase("HOTEL ID") || ResReq[1].trim().equalsIgnoreCase("HOTEL ID(10)") || ResReq[1].trim().equalsIgnoreCase("HOTEL ID(25)")) {
				Req.setSearchType("HOTELCODE");
				String[] hotelcodes = ResReq[2].trim().replace(".0", "").split(";");
				Req.setCode(hotelcodes);

			} else if (ResReq[1].trim().equalsIgnoreCase("CITY CODE")) {
				Req.setSearchType("CITYCODE");
				String[] codes = ResReq[2].trim().replace(".0", "").split(";");
				Req.setCode(codes);

			} else if (ResReq[1].trim().equalsIgnoreCase("HOTEL GROUP CODE")) {
				Req.setSearchType("HOTELGROUPCODE");
				String[] codes = ResReq[2].trim().replace(".0", "").split(";");
				Req.setCode(codes);
			} else if (ResReq[1].trim().equalsIgnoreCase("IATA CODE")) {
				Req.setSearchType("IATACODE");
				String[] codes = ResReq[2].trim().replace(".0", "").split(";");
				Req.setCode(codes);
			} else {
				System.out.println("Not Enterd Correctly Search type");
			}

			Req.setUserName(ResReq[8].trim());
			Req.setPassword(ResReq[9].trim());
			Req.setCheckin(ResReq[3].trim());
			Req.setCheckout(ResReq[4].trim());
			Req.setNoofNights(ResReq[5].trim().replace(".0", ""));
			Req.setNoOfRooms(ResReq[6].trim().replace(".0", ""));
			Req.setState(ResReq[10].trim());
			Req.setCountry(ResReq[11].trim());

			String[] RoomDetails = ResReq[7].trim().split(";");
			ArrayList<Room> roomList = new ArrayList<Room>();

			for (int j = 0; j < RoomDetails.length; j++) {

				Room r = new Room();

				if (RoomDetails[j].startsWith("[")) {
					// String code = "[{1,2,[5,6]};{1,3,[5,6,7]};{1,0,[]}]";
					r.setRoomTypeID("0");					
					
					if(ResReq[15].isEmpty() || ResReq[15].split("\\,")[3].trim() == null || ResReq[15].split("\\,")[3].trim().isEmpty()){
						r.setBedTypeID("0");
					}else{
						r.setBedTypeID(ResReq[15].split("\\,")[3].trim());
					}
					
					r.setAdultsCount(RoomDetails[j].substring(2, 3).trim());
					r.setChildCount(RoomDetails[j].substring(4, 5).trim());

					if (!"0".equals(r.getChildCount()) || r.getChildCount().isEmpty()) {
						String[] childAges = RoomDetails[j].substring(7).split(",");
						childAges[childAges.length - 1] = childAges[childAges.length - 1].replace(childAges[childAges.length - 1], childAges[childAges.length - 1].substring(0, 1));
						r.setChildAges(childAges);
					} else {
						System.out.println("Child Count is Zero");
					}
				} else {
					r.setRoomTypeID("0");
					
					if(ResReq[15].isEmpty() || ResReq[15].split("\\,")[3].trim() == null || ResReq[15].split("\\,")[3].trim().isEmpty()){
						r.setBedTypeID("0");
					}else{
						r.setBedTypeID(ResReq[15].split("\\,")[3].trim());
					}
					
					r.setAdultsCount(RoomDetails[j].substring(1, 2).trim());
					r.setChildCount(RoomDetails[j].substring(3, 4).trim());
					if (!"0".equals(r.getChildCount()) || r.getChildCount().isEmpty()) {

						String[] childAges = RoomDetails[j].substring(6).trim().split(",");
						childAges[childAges.length - 1] = childAges[childAges.length - 1].replace(childAges[childAges.length - 1], childAges[childAges.length - 1].substring(0, 1).trim());
						r.setChildAges(childAges);

					} else {
						System.out.println("invalid room details Enterd or Child Count is Zero");
					}
				}
				roomList.add(r);
			}
			//

			//
			Req.setRoomlist(roomList);
			Req.setUserName(ResReq[8].trim());
			Req.setPassword(ResReq[9].trim());
			//Req.setConfType(ConfirmationType.getConfirmationType(ResReq[12].trim()));
			//Req.setCurrency(ResReq[13].trim());
			Req.setUserComment(ResReq[18].trim());
			Req.setHotelComment(ResReq[19].trim());
			LocalDateTime date = LocalDateTime.now();
			DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
			Req.setReservationDetailsTimeStamp(date.format(formatter));
			// Res Request building goes here
			ResReqList[i][0] = Req;
		}

		return ResReqList;

	}

}
package com.vc.model;

public class QPXExpressAirlineReservation {
	
	private Request request = new Request();
	private Slice[] slice = new Slice[2];
	private int solutions;
	
	public class Request {
		private Passengers passengers = new Passengers();

		public Passengers getPassengers() {
			return passengers;
		}
	}
	
	public class Passengers {
		private int adultCount;

		public int getAdultCount() {
			return adultCount;
		}

		public void setAdultCount(int adultCount) {
			this.adultCount = adultCount;
		}
		
	}
	
	public class Slice {
		private String origin;
		private String destination;
		private String date;
		public String getOrigin() {
			return origin;
		}
		public void setOrigin(String origin) {
			this.origin = origin;
		}
		public String getDestination() {
			return destination;
		}
		public void setDestination(String destination) {
			this.destination = destination;
		}
		public String getDate() {
			return date;
		}
		public void setDate(String date) {
			this.date = date;
		}
		
	}

	public Request getRequest() {
		return request;
	}

	public void setRequest(Request request) {
		this.request = request;
	}

	public int getSolutions() {
		return solutions;
	}

	public void setSolutions(int solutions) {
		this.solutions = solutions;
	}

	public Slice[] getSlice() {
		return slice;
	}

}
package com.cirium.interview.airport;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;

public class Airport {
	private static final String AIRPORT_CODE = "Airport code";
	private static final String AIRPORT_NAME = "Airport name";
	private static final String TIME_ZONE = "Time zone";
	
	private final String airportCode;
	private final String airportName;
	private final ZoneId timeZone;
	
	public Airport(String airportCode, String airportName, ZoneId timeZone) {
		this.airportCode = airportCode == null ? null : airportCode.trim();
		this.airportName = airportName == null ? null : airportName.trim();
		this.timeZone = timeZone;
	}
	
	public String getAirportCode() {
		return airportCode;
	}
	
	public String getAirportName() {
		return airportName;
	}
	
	public ZoneId getTimeZone() {
		return timeZone;
	}
	
	public ZonedDateTime convertToZone(LocalDateTime date) {
		if (date == null) {
			return null;
		}
		return date.atZone(timeZone);
	}
	
	boolean isValid() {
		return airportCode != null && !airportCode.isEmpty() && timeZone != null;
	}
	
	static Airport fromMap(Map<String, String> values) {
		return new Airport(values.get(AIRPORT_CODE), values.get(AIRPORT_NAME), getTimeZoneFromString(values.get(TIME_ZONE)));
	}
	
	private static ZoneId getTimeZoneFromString(String timeZone) {
		try {
			return timeZone == null || timeZone.isEmpty() ? null : ZoneId.of(timeZone.trim());
		} catch (Throwable t) {
			System.err.println(String.format("Unable to parse TimeZone [%s]", timeZone.trim()));
			return null;
		}
	}
}
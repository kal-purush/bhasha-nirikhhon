///////////////////////////////////////////////////////////////////////////////
//                  
// Main Class File:  Scheduler.java
// File:             Event.java
// Semester:         CS 367 Fall 2015
//
// Author:           Andrew Zietlow arzietlow@wisc.edu
// CS Login:         azietlow
// Lecturer's Name:  Jim Skrentny
// Lab Section:      Lecture 1
//
//
// Pair Partner:     N/A
//
// External Help:   None
//////////////////////////// 80 columns wide //////////////////////////////////

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Holds the relevant information related to an event happening within
 * a specific Resource 
 *
 * <p>Bugs: none known
 *
 * @author azietlow
 */
public class Event implements Interval{

	//Class vars
	private String org;        //name of organization responsible for this event
	private String name;       //name of this event
	private String resource;   //resource this event uses
	private String descrip;    //brief summary of this event
	
	//cannot be negative
	private long end; 	       //later dates are larger 
	private long start;	       //earlier dates are smaller

	/**
	 * Constructs a new Event object
	 * @throws IllegalArgumentException if the given parameters are invalid
	 */
	Event(long start, long end, String name, String resource, 
			String organization, String description){

		this.org      = organization;
		this.name 	  = name;
		this.resource = resource;
		this.descrip  = description;
		this.end 	  = end;
		this.start    = start;
		if (this.checkArgs() == false) throw new IllegalArgumentException();
		//An IlegalArgumentException for an Event constructor is thrown
		//when start > end and any of the other string parameters are null.
	}

	/**
	 * Accessor for this Event's start var
	 * @return when this Event begins
	 */
	@Override
	public long getStart(){
		return this.start;
	}

	/**
	 * Accessor for this Event's end var
	 * @return When this Event ends
	 */
	@Override
	public long getEnd(){
		return this.end;
	}

	/**
	 * Accessor for this Event's org var
	 * @return this Event's organization
	 */
	public String getOrganization(){
		return this.org;
	}

	/**
	 * Compares two events of type Interval
	 * @param (o) the other interval to compare this interval with
	 * @return Returns -1 if the start timestamp of this interval-type event is 
	 * less than the start timestamp of the other interval-type event, 
	 * otherwise returns 1.
	 */
	@Override
	public int compareTo(Interval o) {
		if (start < o.getStart()) return -1;
		if (start > o.getStart()) return 1;
		else return 0;
	}

	/**
	 * Checks to see if two Events are Equal
	 * @param (e) the other event to compare this event with
	 * @return Whether two Events have the same starting timestamp
	 */
	public boolean equals(Event e) {
		return (e.getStart() == start);
	}

	@Override
	/**
	 * Determines whether there is an overlap between the two intervals.
	 * @param e the other interval to compare this interval with
	 * @return if there is an overlap between the intervals.
	 */
	public boolean overlap(Interval e) {
		//could be combined into a single "or" statement,
		//but that might look convoluted 
		boolean result = false;
		if ((e.getStart() <= this.getStart()) && (this.getStart() 
				<= e.getEnd())) {
			result = true;
		}
		else if ((this.getStart() <= e.getStart()) && (e.getStart() 
				<= this.getEnd())) {
			result = true;
		}
		return result;
	}

	/**
	 * Builds an Event object's string representation for output
	 * @return the string representation of this Event
	 */
	public String toString(){
		return name + "\nBy: " + org + "\nIn: " + resource +
				"\nStart: " + convertToTime(start) + "\nEnd: " + 
				convertToTime(end) + "\n" + "Description: " + descrip;
	}

	/**
	 * Used for determining whether an Event object has valid parameters
	 * @return if the Event is valid
	 */
	private boolean checkArgs() {
		if (this.org == null)      return false;
		if (this.name == null)     return false;
		if (this.resource == null) return false;
		if (this.descrip == null)  return false;
		if (this.end < this.start) return false;
		if ((this.end < 0) || (this.start < 0)) return false;
		return true;
	}

	/**
	 * Converts an Event's long timestamps into formatted dates
	 * @param (time) an amount of time in seconds since 1/1/1970
	 * @return time in string format
	 */
	private String convertToTime(long time){
		String result = "";
		Format formatter = new SimpleDateFormat("MM/dd/yyyy,HH:mm");
		try{
			Date date = new Date(time * 1000);
			result = formatter.format(date);
		}catch(Exception e){
			System.out.println("Dates are not formatted correctly.");
		}
		return result;
	}
}
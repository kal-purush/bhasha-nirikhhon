package application;

import java.util.ArrayList;
import java.util.Collections;

public class MilkList extends ArrayList<MilkData> {

	MilkList(){
	super();	
	}

	
	/**
	 * add an instance of MilkData
	 * @param data - a MilkData instance 
	 */
	void addMilk(MilkData data) {
		super.add(data);
	}
	
	/**
	 * Add all in an array of MilkData into the list 
	 * @param data - an ArrayList of MilkData
	 */
	void addMilk(ArrayList<MilkData> data) {
		super.addAll(data);
	}
	
	
	/**
	 * creates an ArrayList with all instances inclusively between the two years entered 
	 * @param begin - starting year
	 * @param end - ending year
	 * @return - MilkList of all MilkData between the starting and ending year
	 */
	MilkList getFromYears(int begin, int end){
		MilkList yearMilk = new MilkList();
		for( MilkData data : this) {
			if(data.getDate().getYear() >= begin && data.getDate().getYear() <= end) {
				yearMilk.add(data);
			}
		}
		return yearMilk;
	}
	
	/**
	 * creates an ArrayList with all instances within the year provided
	 * @param year
	 * @return - MilkList of all MilkData during the year
	 */
	MilkList getFromYear(int year) {
		return getFromYears(year, year);
	}
	
	/**
	 * Returns all milk values over a span of months
	 * @param year
	 * @param begin
	 * @param end
	 * @return MilkList
	 */
	MilkList getFromMonths(int year, int begin, int end){
		MilkList monthMilk = new MilkList();
		for( MilkData data : this) {
			if(data.getDate().getMonthValue() >= begin && data.getDate().getMonthValue() <= end) {
				monthMilk.add(data);
			}
		}
		return monthMilk;		
	}
	
	/**
	 * gets all milk values from a specific month
	 * @param year
	 * @param month
	 * @return - MilkList
	 */
	MilkList getFromMonth(int year, int month){
		return getFromMonths(year, month, month);
	}
	
	/**
	 * gets all milk values from a specific farm
	 * @param farmID
	 * @return - MilkList
	 */
	MilkList getFromFarm(String farmID){
		 MilkList farmMilk = new MilkList();
		for(MilkData data : this) {
			if(data.getFarmName().equals(farmID)) {
				farmMilk.add(data);
			}
		}
		return farmMilk;
	}
	
	/**
	 * finds the MilkData with the minimum milk weight in the list
	 * @return - MilkData
	 */
	MilkData getMin() {
		return Collections.min(this, (data1, data2) -> (int) (data1.getMilkWeight() - data2.getMilkWeight()));
	}
	
	/**
	 * finds the MilkData with the maximum milk weight in the list
	 * @return - MilkData
	 */
	MilkData getMax() {
		return Collections.max(this, (data1, data2) -> (int) (data1.getMilkWeight() - data2.getMilkWeight()));
	}
	
	/**
	 * find the mean value of milk weight in a MilkList
	 * @return - mean value
	 */
	double getMean() {
		double mean = 0;
		for(MilkData data : this) {
			mean += data.getMilkWeight();
		}
		return mean / this.size(); 
	}
}
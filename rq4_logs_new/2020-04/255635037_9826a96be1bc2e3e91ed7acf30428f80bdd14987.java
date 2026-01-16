package application;

import java.util.ArrayList;

public interface IMilkList {

	/**
	 * @param data - MilkData to be added
	 */
	public void addMilk(MilkData data);
	/**
	 * @param data ArrayList<MilkData> to be added
	 */
	public void addMilk(ArrayList<MilkData> data);
	/**
	 * @param begin year
	 * @param end year
	 * @return IMilkList of all MilkDatas within those years - inclusive of endpoints
	 */
	public IMilkList getFromYears(int begin, int end);
	/**
	 * @param year
	 * @return IMilkList of all MilkDatas in that year
	 */
	public IMilkList getFromYear(int year);
	/**
	 * @param year
	 * @param begin month
	 * @param end month
	 * @return IMilkList of all MilkDatas within those months - inclusive of endpoints
	 */
	public IMilkList getFromMonths(int year, int begin, int end);
	/**
	 * @param year
	 * @param month
	 * @return IMilkList of all MilkDatas in that month
	 */
	public IMilkList getFromMonth(int year, int month);
	/**
	 * @param farmID
	 * @return IMilkList of all MilkDatas in that farm
	 */
	public IMilkList getFromFarm(String farmID);
	/**
	 * @return MilkData with least milk in the collection
	 */
	public MilkData getMin();
	/**
	 * @return MilkData with the most milk in the collection
	 */
	public MilkData getMax();
	/**
	 * @return the mean value of milk in the collection
	 */
	public double getMean();
	/**
	 * @return sum of milk in the collection
	 */
	public double getSum();
}
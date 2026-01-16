package ro.sci.cms;

import java.util.Collection;

public interface CompanyManagementInterface {

	/**
	 * Print all the employees.
	 */
	void printOfAllEmployees();

	/**
	 * This method generates the list of employees with parking space.
	 * 
	 * @return list of these employees
	 */
	Collection<Employee> generateListOfEmployeesWithParkingSpace();

	/**
	 * This method print the list of employees with parking space.
	 * 
	 * @param list
	 *            of employees with parking space - to be print
	 */
	void printOfEmploiesWithParkingSpace(Collection<Employee> employWithParkingSpace);
	
	/**
	 * This method set number of parking spaces.
	 * 
	 * @param numberOfParkingSpaces
	 *            
	 */

	void setNumberOfParkingSpaces(int numberOfParkingSpaces);


}
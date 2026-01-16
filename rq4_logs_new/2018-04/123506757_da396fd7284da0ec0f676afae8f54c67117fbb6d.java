
import java.time.LocalDate;

/**
 * @author Amy Larson amlarson10@dmacc.edu 2018-03-15 CIS152 This class contains
 *         details about a job listing
 */

public class JobListing {

	private String company;
	private String jobTitle;
	private String location;
	private String notes;
	private LocalDate foundDate;//date job listing was first found
	private LocalDate completedDate; //date job was applied for and moved to completed queue


	/**
	 * constructor with no fields
	 */
	public JobListing() {
	}

	/**
	 * constructor
	 * 
	 * @param company company name
	 * @param jobTitle job title
	 * @param location location of job (e.g. city, state)
	 * @param foundDate date the listing was first viewed
	 * @param notes any notes about the listing, including deadline if known
	 */
	public JobListing(String company, String jobTitle, String location, LocalDate foundDate, String notes) {
		setCompany(company);
		setJobTitle(jobTitle);
		setLocation(location);
		setFoundDate(foundDate);
		setNotes(notes); 
	}

	/**
	 * getter for company
	 * 
	 * @return company 
	 */
	public String getCompany() {
		return company;
	}

	/**
	 * setter for company
	 * 
	 * @param company 
	 */
	public void setCompany(String company) {
		this.company = company;
	}

	/**
	 * getter for jobTitle
	 * 
	 * @return jobTitle
	 */
	public String getJobTitle() {
		return jobTitle;
	}

	/**
	 * setter for jobTitle
	 * 
	 * @param jobTitle
	 */
	public void setJobTitle(String jobTitle) {
		this.jobTitle = jobTitle;
	}

	/**
	 * getter for location
	 * 
	 * @return location
	 */
	public String getLocation() {
		return location;
	}

	/**
	 * setter for location
	 * 
	 * @param location 
	 */
	public void setLocation(String location) {
		this.location = location;
	}

	/**
	 * getter for notes
	 * 
	 * @return notes
	 */
	public String getNotes() {
		return notes;
	}

	/**
	 * setter for notes
	 * 
	 * @param notes
	 */
	public void setNotes(String notes) {
		this.notes = notes;
	}

	/**
	 * getter for foundDate
	 * 
	 * @return foundDate
	 */
	public LocalDate getFoundDate() {
		return foundDate;
	}

	/**
	 * setter for foundDate 
	 * 
	 * @param year
	 * @param month
	 * @param day
	 */
	public void setFoundDate(int year, int month, int day) {
		this.foundDate = LocalDate.of(year, month, day);
	}
	
	/**setter for foundDate
	 * @param aDate
	 */
	private void setFoundDate(LocalDate aDate) {
		this.foundDate = aDate;
	}

	/**getter for completedDate
	 * @return
	 */
	public LocalDate getCompletedDate() {
		return completedDate;
	}

	/**setter for completedDate
	 * @param completedDate
	 */
	public void setCompletedDate(LocalDate completedDate) {
		this.completedDate = completedDate;
	}

	/**
	 *
	 * @param year
	 * @param month
	 * @param day
	 */
	public void setCompletedDate(int year, int month, int day) {
		this.completedDate = LocalDate.of(year, month, day);
	}
	
	

	/**
	 * overrides toString() to 
	 * @see java.lang.Object#toString()
	 * @return description string with formatted main information from this JobListing
	 */
	public String toString() {
		String description = (jobTitle + " at " + company + " in " + location + ". \nFound: "+getFoundDate()+ ". \nNotes: "+notes);
		return description;
	}
}
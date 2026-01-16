package application;

public class Location
{
private String locationName;
private String description;
	
	//constructor 
	public Location(String name, String descr)
	{
		this.locationName = name;
		this.description = descr;
	}
	
	//set function for location name
	public void setLocationName(String name)
	{
		this.locationName = name;
	}
	
	//get function for location name
	public String getLocationName()
	{
		return this.locationName;
	}
	
	//set function for location description
	public void setLocationDescr(String descript)
	{
		this.description = descript;
	}
	
	//get function for location description
	public String getLocationDescr()
	{
		return this.description;
	}
}
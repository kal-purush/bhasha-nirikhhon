
/**
* This class creates the Place object.
**/

class Place implements Comparable<Place> {
	
	private String name;
	private double latitude;
	private double longitude;
	private int freq;
	
	public Place() 
	{	
		this(null, 0, 0);
	}
	
	public Place(String name, double latitude, double longitude) 
	{
		this.name = name;
		this.latitude = latitude;
		this.longitude = longitude;
		freq = 0;
	}

/**
* This method overrides the Object equals() method and defines equality solely
* in terms of name.
**/
	public boolean equals(Place that) 
	{
		return this.name.equals(that.name);
	}

/**
* This method returns the Place's latitude.
* @return double latitude
**/			
	public double getLat() 
	{
		return latitude;
	}

/**
* This method returns the Place's longitude.
* @return double longitude
**/		
	public double getLong()
	{
		return longitude;
	}

/**
* This method  returns the Place's frequency.
* @return int freq
**/		
	public int getFreq()
	{
		return freq;
	}

/**
* This method returns the Place's name.
* @return String place
**/		
	public String getName()
	{
		return name;
	}

/**
* This method returns the Place as a String.
* @return String name
**/		
	public String toString()
	{
		return name + "," + latitude + "," + longitude;
	}

/**
* This method allows users to increment the frequency.
**/
	public void addFreq()
	{
		freq++;
	}

/**
* This method defines Place comparability in terms of frequency.
* @param Place that
* @return int 1 if greater, 0 if equal, -1 if less
**/
	public int compareTo(Place that)
	{
        if  (this.freq < that.freq)  return 1;
        else if (this.freq > that.freq ) return -1;
        else return 0;
    }  
}
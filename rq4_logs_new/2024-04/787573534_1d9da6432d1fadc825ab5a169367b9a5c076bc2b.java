package application;

public class CreateAsset 
{
	private String assetName;
	private String purchaseDate, warrantyDate;
	private String description;
	private String purchaseVal;
	private String category, location;
	
	public CreateAsset(String name, String purDate, String expDate, String descr, 
			String cost, String cat, String loc)
	{
		this.assetName = name;
		this.purchaseDate = purDate;
		this.warrantyDate = expDate;
		this.description = descr;
		this.purchaseVal = cost;
		this.category = cat;
		this.location = loc;
	}
	
	public void setAssetName(String name)
	{
		this.assetName = name;
	}
	
	public void setPurchDate(String purDate)
	{
		this.purchaseDate = purDate;
	}
	
	public void setExpDate(String expDate)
	{
		this.warrantyDate = expDate;
	}
	
	public void setDescription(String descr)
	{
		this.description = descr;
	}
	
	public void setPurchVal(String cost)
	{
		this.purchaseVal = cost;
	}
	
	public void setCategory(String cat)
	{
		this.category = cat;
	}
	
	public void setLocation(String loc)
	{
		this.location = loc;
	}

}
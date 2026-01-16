package team01.pojos.US_17_CreateTestItems;

import java.io.Serializable;

public class TestItemResponsePOJO implements Serializable {
	private String createdBy;
	private String createdDate;
	private int id;
	private String name;
	private String description;
	private int price;
	private String defaultValMin;
	private String defaultValMax;

	public TestItemResponsePOJO() {
	}

	public TestItemResponsePOJO(String createdBy, String createdDate, int id, String name, String description, int price, String defaultValMin, String defaultValMax) {
		this.createdBy = createdBy;
		this.createdDate = createdDate;
		this.id = id;
		this.name = name;
		this.description = description;
		this.price = price;
		this.defaultValMin = defaultValMin;
		this.defaultValMax = defaultValMax;
	}

	public void setCreatedBy(String createdBy){
		this.createdBy = createdBy;
	}

	public String getCreatedBy(){
		return createdBy;
	}

	public void setCreatedDate(String createdDate){
		this.createdDate = createdDate;
	}

	public String getCreatedDate(){
		return createdDate;
	}

	public void setId(int id){
		this.id = id;
	}

	public int getId(){
		return id;
	}

	public void setName(String name){
		this.name = name;
	}

	public String getName(){
		return name;
	}

	public void setDescription(String description){
		this.description = description;
	}

	public String getDescription(){
		return description;
	}

	public void setPrice(int price){
		this.price = price;
	}

	public int getPrice(){
		return price;
	}

	public void setDefaultValMin(String defaultValMin){
		this.defaultValMin = defaultValMin;
	}

	public String getDefaultValMin(){
		return defaultValMin;
	}

	public void setDefaultValMax(String defaultValMax){
		this.defaultValMax = defaultValMax;
	}

	public String getDefaultValMax(){
		return defaultValMax;
	}

	@Override
 	public String toString(){
		return 
			"TestItemResponsePOJO{" + 
			"createdBy = '" + createdBy + '\'' + 
			",createdDate = '" + createdDate + '\'' + 
			",id = '" + id + '\'' + 
			",name = '" + name + '\'' + 
			",description = '" + description + '\'' + 
			",price = '" + price + '\'' + 
			",defaultValMin = '" + defaultValMin + '\'' + 
			",defaultValMax = '" + defaultValMax + '\'' + 
			"}";
		}
}
package jl.forthem.dto;

public class ItemDTO {
	private Integer id;
	
	private String name;
	
	private Integer categoryId;

	

	public Integer getId() {
		return id;
	}



	public void setId(Integer id) {
		this.id = id;
	}



	public String getName() {
		return name;
	}



	public void setName(String name) {
		this.name = name;
	}



	public Integer getCategoryId() {
		return categoryId;
	}



	public void setCategoryId(Integer categoryId) {
		this.categoryId = categoryId;
	}



	@Override
	public String toString() {
		return "ItemDTO [itemId=" + id + ", itemName=" + name + ", categoryId=" + categoryId + "]";
	}
	
	

}
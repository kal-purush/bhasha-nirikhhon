package jl.forthem.dto;

import java.util.List;

import jl.forthem.models.Item;

/**
 * @author jl
 * Class added to suppress a potential vulnerability detected by SonarQube
 *
 */
public class CategoryDTO {
	@Override
	public String toString() {
		return "CategoryDTO [id=" + id + ", name=" + name + ", items=" + items + "]";
	}

	private Integer id;
	
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

	public List<Item> getItems() {
		return items;
	}

	public void setItems(List<Item> items) {
		this.items = items;
	}

	private String name;
	
	private List<Item> items;
	
}
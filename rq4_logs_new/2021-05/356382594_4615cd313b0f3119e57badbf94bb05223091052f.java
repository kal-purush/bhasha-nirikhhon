package de.cookiebook.restservice;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.Table;

@Entity
@Table(name = "tags6")
public class Tag {
	
	
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Id
    @Column(name = "id")
    private long id;
	private String title;
	@ManyToMany(mappedBy="tags")
    private List<Recipe> taggedRecipes = new ArrayList<Recipe>(); //= Collections.<Recipe>emptyList();
	
	public Tag(){};
	
	public void setTitle(String title) {
		this.title = title;
	}

	public Tag(String title) {
		this.title = title;
	}
	
	public Long getId() {
		return id;
	}
	public String getTitle() {
		return title;
	}
	
	public void addRecipe(Recipe recipe) {
		
		System.out.println(this.id);
		taggedRecipes.add(recipe);
		recipe.addTag(this);
	}

}
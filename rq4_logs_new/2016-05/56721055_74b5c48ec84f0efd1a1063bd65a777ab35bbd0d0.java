package com.dotcook.application;

import com.dotcook.connection.Connection;

public class Category extends Connection{
	
	private int idCategory;
	private String nameCategory;
	private String descriptionCategory;
	private int positionCategory;
	
	
	public Category(){
		
	}


	public int getIdCategory() {
		return idCategory;
	}


	public void setIdCategory(int idCategory) {
		this.idCategory = idCategory;
	}


	public String getNameCategory() {
		return nameCategory;
	}


	public void setNameCategory(String nameCategory) {
		this.nameCategory = nameCategory;
	}


	public String getDescriptionCategory() {
		return descriptionCategory;
	}


	public void setDescriptionCategory(String descriptionCategory) {
		this.descriptionCategory = descriptionCategory;
	}


	public int getPositionCategory() {
		return positionCategory;
	}


	public void setPositionCategory(int positionCategory) {
		this.positionCategory = positionCategory;
	}

}
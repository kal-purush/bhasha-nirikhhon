package com.tia102g3.food.model;

public class FoodVO implements java.io.Serializable {
	private Integer foodNumber;
	private Integer foodTypeNumber;
	private String foodName;
	private Integer foodCalories;
	public Integer getFoodNumber() {
		return foodNumber;
	}
	public void setFoodNumber(Integer foodNumber) {
		this.foodNumber = foodNumber;
	}
	public Integer getFoodTypeNumber() {
		return foodTypeNumber;
	}
	public void setFoodTypeNumber(Integer foodTypeNumber) {
		this.foodTypeNumber = foodTypeNumber;
	}
	public String getFoodName() {
		return foodName;
	}
	public void setFoodName(String foodName) {
		this.foodName = foodName;
	}
	public Integer getFoodCalories() {
		return foodCalories;
	}
	public void setFoodCalories(Integer foodCalories) {
		this.foodCalories = foodCalories;
	}
	
}
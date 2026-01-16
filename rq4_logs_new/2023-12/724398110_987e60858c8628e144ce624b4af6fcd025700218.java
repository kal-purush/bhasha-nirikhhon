package com.example.a327finalprojectcalorietracker;

import com.google.gson.annotations.SerializedName;

public class foodItem {
    @SerializedName("fdcId")
    private int fdcId;

    @SerializedName("availableDate")
    private String availableDate;

    @SerializedName("brandOwner")
    private String brandOwner;

    @SerializedName("dataSource")
    private String dataSource;

    @SerializedName("dataType")
    private String dataType;

    @SerializedName("description")
    private String description;

    @SerializedName("foodClass")
    private String foodClass;

    @SerializedName("ingredients")
    private String ingredients;

    @SerializedName("servingSize")
    private float servingSize;

    @SerializedName("servingSizeUnit")
    private String servingSizeUnit;

    @SerializedName("labelNutrients")
    private labelNutrients labelNutrients;

    // getters and setters for each field

    public int getFdcId() {
        return fdcId;
    }

    public void setFdcId(int fdcId) {
        this.fdcId = fdcId;
    }

    public String getAvailableDate() {
        return availableDate;
    }

    public void setAvailableDate(String availableDate) {
        this.availableDate = availableDate;
    }

    public String getBrandOwner() {
        return brandOwner;
    }

    public void setBrandOwner(String brandOwner) {
        this.brandOwner = brandOwner;
    }

    public String getDataSource() {
        return dataSource;
    }

    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getFoodClass() {
        return foodClass;
    }

    public void setFoodClass(String foodClass) {
        this.foodClass = foodClass;
    }

    public String getIngredients() {
        return ingredients;
    }

    public void setIngredients(String ingredients) {
        this.ingredients = ingredients;
    }

    public float getServingSize() {
        return servingSize;
    }

    public void setServingSize(float servingSize) {
        this.servingSize = servingSize;
    }

    public String getServingSizeUnit() {
        return servingSizeUnit;
    }

    public void setServingSizeUnit(String servingSizeUnit) {
        this.servingSizeUnit = servingSizeUnit;
    }

    public com.example.a327finalprojectcalorietracker.labelNutrients getLabelNutrients() {
        return labelNutrients;
    }

    public void setLabelNutrients(com.example.a327finalprojectcalorietracker.labelNutrients labelNutrients) {
        this.labelNutrients = labelNutrients;
    }
}
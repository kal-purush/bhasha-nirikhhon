package com.example.a327finalprojectcalorietracker;

import com.google.gson.annotations.SerializedName;
public class FoodNutrient {



    public int getNutrientId() {
        return nutrientId;
    }

    public String getNutrientName() {
        return nutrientName;
    }

    public String getNutrientNumber() {
        return nutrientNumber;
    }

    public String getUnitName() {
        return unitName;
    }

    public float getValue() {
        return value;
    }

    public int getRank() {
        return rank;
    }

    public int getIndentLevel() {
        return indentLevel;
    }

    public int getFoodNutrientId() {
        return foodNutrientId;
    }

    @SerializedName("nutrientId")
    private int nutrientId;

    @SerializedName("nutrientName")
    private String nutrientName;

    @SerializedName("nutrientNumber")
    private String nutrientNumber;

    @SerializedName("unitName")
    private String unitName;

    @SerializedName("value")
    private float value;

    @SerializedName("rank")
    private int rank;

    @SerializedName("indentLevel")
    private int indentLevel;

    @SerializedName("foodNutrientId")
    private int foodNutrientId;

    // getters and setters for each field

    public String displayNutrients() {
        return "FoodNutrient {" +
                "\nnutrientId=" + nutrientId +
                ", \nnutrientName='" + nutrientName + '\'' +
                ", \nnutrientNumber='" + nutrientNumber + '\'' +
                ", \nunitName='" + unitName + '\'' +
                ", \nvalue=" + value +
                ", \nrank=" + rank +
                ", \nindentLevel=" + indentLevel +
                ", \nfoodNutrientId=" + foodNutrientId +
                "\n}";
    }


}
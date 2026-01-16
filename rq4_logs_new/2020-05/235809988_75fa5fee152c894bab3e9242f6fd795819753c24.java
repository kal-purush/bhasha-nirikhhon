package com.group4sweng.scranplan.MealPlanner.Ingredients;

public class Ingredient {

    private String mName;
    private int mIcon = -1;

    private String mWarning = null;
    private boolean SHOW_PORTION_CONVERT_WARNING = false;

    public Ingredient(String mName){
        this.mName = mName;
    }

    public Ingredient(String mName, Warning mWarning){
        this(mName);
        setWarning(mWarning);
    }

    public Ingredient(String mName, int mIcon, Warning mWarning){
        this(mName, mWarning);
        this.mIcon = mIcon;
    }

    public void setName(String mName){
        this.mName = mName;
    }

    public String getName() {
        return mName;
    }

    public void setIcon(int mIcon) {
        this.mIcon = mIcon;
    }

    public int getIcon() {
        return mIcon;
    }

    public void setWarning(Warning mWarning){
        switch(mWarning){
            case NONE:
                this.mWarning = null;
            case FAILED:
                this.mWarning = "Failed to convert portion amount for ingredient";
            case ESTIMATE:
                this.mWarning = "Portions for alcohol & herbs are a best estimate";
        }
    }

    public String getWarning(){
        return mWarning;
    }
}
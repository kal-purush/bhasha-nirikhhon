package com.hackathon.feedmid.feedmid;

import android.content.Context;
import android.util.AttributeSet;
import android.view.LayoutInflater;
import android.widget.RelativeLayout;
import android.widget.TextView;
import android.widget.CheckBox;

import java.text.DecimalFormat;

public class RecipeItem extends RelativeLayout {

    private TextView name, approxDollars, serves, peopleServed, people, price;
    private CheckBox checkBox;

    public RecipeItem(Context context){
        super(context);
        initializeViews(context);
    }

    public RecipeItem (Context context, AttributeSet attrs) {
        super(context, attrs);
        initializeViews(context);
    }

    public RecipeItem(Context context,
                       AttributeSet attrs,
                       int defStyle) {
        super(context, attrs, defStyle);
        initializeViews(context);
    }

    private void initializeViews(Context context){
        LayoutInflater inflater = (LayoutInflater) context.getSystemService(Context.LAYOUT_INFLATER_SERVICE);
        inflater.inflate(R.layout.sample_recipe_view, this);
    }

    public void setName(String newName){
        name = (TextView)this.findViewById(R.id.recipeName);
        name.setText(newName);
    }

    public void setPeopleServed(String range){
        peopleServed = (TextView)this.findViewById(R.id.servings);
        peopleServed.setText(range);
    }

    public void setPrice(float p){
        price = (TextView)this.findViewById(R.id.price);
        DecimalFormat df = new DecimalFormat();
        df.setMaximumFractionDigits(2);
        df.setMinimumFractionDigits(2);
        price.setText(df.format(p));
    }

    public boolean isChecked(){
        checkBox = (CheckBox)this.findViewById(R.id.checkBox);
        return checkBox.isChecked();
    }


}
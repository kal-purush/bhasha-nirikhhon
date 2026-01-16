package com.mlee138.mykitchen_cs441;

import android.app.Activity;
import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ArrayAdapter;
import android.widget.TextView;

import java.util.ArrayList;

public class AdapterFoodItem extends ArrayAdapter<FoodItem> {

    private Activity activity;
    private ArrayList<FoodItem> items;
    private static LayoutInflater inflater = null;

    public AdapterFoodItem (Activity activityIn, int textViewResourceID, ArrayList<FoodItem> itemsIn) {
        super (activityIn, textViewResourceID, itemsIn);
        try {
            this.activity = activityIn;
            this.items = itemsIn;

            this.inflater = (LayoutInflater) activityIn.getSystemService(Context.LAYOUT_INFLATER_SERVICE);
        } catch (Exception e){

        }
    }

    public int getCount() {
        return items.size();
    }

    public FoodItem getItem (FoodItem position){
        return position;
    }

    public long getItemId(int position) {
        return position;
    }

    public static class ViewHolder {
        public TextView display_name;
        public TextView display_quantity;
    }

    public View getView (int position, View convertView, ViewGroup parent){
        View vi = convertView;
        final ViewHolder holder;
        try {
            if (convertView == null) {
                vi = inflater.inflate(R.layout.kitchen_content, null);
                holder = new ViewHolder();

                //holder.display_name = (TextView) vi.findViewById(R.id.food_name);
                //holder.display_quantity = (TextView) vi.findViewById(R.id.food_quantity);


                vi.setTag(holder);
            } else {
                holder = (ViewHolder) vi.getTag();
            }



            holder.display_name.setText(items.get(position).getName());
            holder.display_quantity.setText(items.get(position).getQuantity());


        } catch (Exception e) {


        }
        return vi;
    }
}
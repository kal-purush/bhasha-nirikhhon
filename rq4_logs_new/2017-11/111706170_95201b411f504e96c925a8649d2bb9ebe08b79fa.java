package com.segproject.niflheimr.projectapplication;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ArrayAdapter;
import android.widget.ImageView;
import android.widget.TextView;

/**
 * Created by pph on 2017/11/28.
 */

public class ChoreCustomAdapter extends ArrayAdapter{
    private final Context context;
    private final String[] myChores;

    public ChoreCustomAdapter(Context context, String[] choreList){
        super(context, R.layout.user_chore_item_layout, choreList);
        this.context = context;
        this.myChores = choreList;
    }
    public View getView(int position, View convertView, ViewGroup parent){
        LayoutInflater inflater = (LayoutInflater)context.getSystemService(Context.LAYOUT_INFLATER_SERVICE);
        View rowView = inflater.inflate(R.layout.user_chore_item_layout, parent, false);
        TextView choreNameTextField = (TextView)rowView.findViewById(R.id.choreName);
        TextView deadLine = (TextView)rowView.findViewById(R.id.deadLine);
        ImageView choreImage = (ImageView) rowView.findViewById(R.id.choreImage);
        choreNameTextField.setText(myChores[position]);
        deadLine.setText("Diedline is at " + myChores[position]);

        return rowView;
    }

}
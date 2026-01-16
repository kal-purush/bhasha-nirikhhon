package com.samkantor.borderdelays;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ArrayAdapter;
import android.widget.TextView;

import java.util.Objects;


/**
 * Created by SAM on 8/6/2016.
 */

public class AdapterPortList extends ArrayAdapter<String> {

    public AdapterPortList(Context context, String[] objects) {
        super(context, 0, objects);
        mInflater = (LayoutInflater) context.getSystemService(Context.LAYOUT_INFLATER_SERVICE);
    }


    private LayoutInflater mInflater;

    public static class Viewholder {
        public TextView nameOfPort;
    }

    public View getView(int position, View convertView, ViewGroup parent) {
        Viewholder holder = new Viewholder();

        if (convertView == null) {
            convertView = mInflater.inflate(R.layout.port_list_row, null);
            TextView title = (TextView) convertView.findViewById(R.id.name_of_port);

            holder.nameOfPort = title;
            convertView.setTag(holder);
        }
        else {
            holder = (Viewholder) convertView.getTag();
        }

        holder.nameOfPort.setText("Hi");

        return convertView;
    }




}
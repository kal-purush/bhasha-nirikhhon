package org.jamaica.pcog.mobile.adapter;

/**
 * Created by ajay's on 28-03-2017.
 */

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ArrayAdapter;
import android.widget.TextView;

import org.jamaica.pcog.mobile.R;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;

import static org.jamaica.pcog.mobile.R.string.name;

public class EventAdapter extends ArrayAdapter<JSONObject>{
    int vg;
    ArrayList<JSONObject> list;
    Context context;
    public EventAdapter(Context context, int vg, ArrayList<JSONObject> list){
        super(context,vg,list);
        this.context=context;
        this.vg=vg;
        this.list=list;
    }
    public View getView(int position, View convertView, ViewGroup parent) {
        LayoutInflater inflater = (LayoutInflater) context.getSystemService(Context.LAYOUT_INFLATER_SERVICE);
        View itemView = inflater.inflate(vg, parent, false);
        TextView txtName=(TextView)itemView.findViewById(R.id.txtName);
        TextView txtDay=(TextView)itemView.findViewById(R.id.txtDay);
        TextView txtMonth=(TextView)itemView.findViewById(R.id.txtMonth);

        try {
            txtName.setText(list.get(position).getString("name"));
            txtDay.setText(list.get(position).getString("day"));
            txtMonth.setText(list.get(position).getString("month"));
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return itemView;
    }

    public int getName() {
        return name;
    }
}
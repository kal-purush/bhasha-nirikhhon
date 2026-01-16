package com.example.gorent.controle;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.BaseAdapter;
import android.widget.TextView;

import com.example.gorent.R;
import com.example.gorent.entidade.Client;

import java.util.ArrayList;

/**
 * Created by orlandoamorim on 15/01/17.
 */

public class ClientAdapter extends BaseAdapter {
    public Context mContext;
    private LayoutInflater mInflater;
    private ArrayList<Client> mDataSource;

    public ClientAdapter(Context context, ArrayList<Client> items) {
        mContext = context;
        mDataSource = items;
        mInflater = (LayoutInflater) mContext.getSystemService(Context.LAYOUT_INFLATER_SERVICE);
    }

    //1
    @Override
    public int getCount() {
        return mDataSource.size();
    }

    //2
    @Override
    public Object getItem(int position) {
        return mDataSource.get(position);
    }

    //3
    @Override
    public long getItemId(int position) {
        return position;
    }

    //4
    @Override
    public View getView(int position, View convertView, ViewGroup parent) {
        // Get view for row item
        View rowView = mInflater.inflate(R.layout.list_item_car, parent, false);

        // Get title element
        TextView titleTextView =
                (TextView) rowView.findViewById(R.id.car_list_title);

        // Get subtitle element
        TextView subtitleTextView =
                (TextView) rowView.findViewById(R.id.car_list_subtitle);

        // Get detail element
        TextView detailTextView =
                (TextView) rowView.findViewById(R.id.car_list_detail);


        // 1
        Client client = (Client) getItem(position);

        // 2
        titleTextView.setText(client.name);
        subtitleTextView.setText(client.adress);
        detailTextView.setText(client.id);

        return rowView;
    }
}

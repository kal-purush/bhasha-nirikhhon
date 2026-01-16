package com.example.expensemanagement.Home.Adapter;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.BaseAdapter;
import android.widget.TextView;

import com.example.expensemanagement.Home.Inventory.Date;
import com.example.expensemanagement.R;

import java.util.List;

public class DateAdapter extends BaseAdapter {

    private Context context;
    private List<Date> dateList;

    public DateAdapter(Context context, List<Date> dateList) {
        this.context = context;
        this.dateList = dateList;
    }

    @Override
    public int getCount() {
        return dateList != null ? dateList.size() : 0;
    }

    @Override
    public Object getItem(int position) {
        return position;
    }

    @Override
    public long getItemId(int position) {
        return position;
    }

    @Override
    public View getView(int position, View convertView, ViewGroup parent) {
        View rootView = LayoutInflater.from(context).inflate(R.layout.item_date,parent,false);

        TextView txtName = rootView.findViewById(R.id.timeTxt);

        txtName.setText(dateList.get(position).getName());
        return rootView;
    }
}
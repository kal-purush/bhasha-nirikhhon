package com.msproject.myhome.mydays;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.BaseAdapter;
import android.widget.TextView;

import java.util.ArrayList;

public class EventListAdapter extends BaseAdapter {
    ArrayList<Event> events;
    Context context;

    public EventListAdapter(ArrayList<Event> events, Context context){
        this.events = events;
        this.context = context;
    }

    @Override
    public int getCount() {
        return events.size();
    }

    @Override
    public Object getItem(int position) {
        return events.get(position);
    }

    @Override
    public long getItemId(int position) {
        return 0;
    }

    @Override
    public View getView(int position, View convertView, ViewGroup parent) {
        View view = LayoutInflater.from(parent.getContext()).inflate(R.layout.event_item, parent, false);
        TextView timeTextView = view.findViewById(R.id.event_time);
        TextView categoryName = view.findViewById(R.id.category_name);
        TextView eventContent = view.findViewById(R.id.event_content);

        timeTextView.setText("0"+events.get(position).getEventNo()+":00");
        categoryName.setText(events.get(position).getCategoryName());
        eventContent.setText(events.get(position).getEventContent());

        return view;
    }
}
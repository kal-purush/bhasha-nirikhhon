package com.app.Regional_News.adapter;

import android.content.Context;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.LinearLayout;
import android.widget.TextView;
import com.app.Regional_News.EventActivity;

import androidx.cardview.widget.CardView;
import androidx.recyclerview.widget.RecyclerView;

import com.app.Regional_News.R;
import com.app.Regional_News.data.Event_cal_listdata;
import com.squareup.picasso.Callback;
import com.squareup.picasso.Picasso;

import java.util.ArrayList;

public class EventlistAdapter extends RecyclerView.Adapter<EventlistAdapter.ViewHolder> {


    private final ArrayList<Event_cal_listdata> clist;
    private final Context mContext;


    //Provide a reference to the views for each data item
    //Complex data items may need more than one view per item, and
    //you provide access to all the views for a data item in a view holder
    public static class ViewHolder extends RecyclerView.ViewHolder {
        //each data item is just a string in this case
        public TextView tv_event_date,event_desc,tv_event_name;
        public CardView cardview;
        public ImageView news_images;
        public LinearLayout event_list_layout;


        public ViewHolder(View v) {
            super(v);
            tv_event_date = v.findViewById(R.id.tv_event_date);
            tv_event_name = v.findViewById(R.id.tv_event_name);
//            event_desc = v.findViewById(R.id.event_desc);
            cardview = v.findViewById(R.id.cardview);
            news_images = v.findViewById(R.id.news_images); // This is where news_images ImageView is initialized
            event_list_layout = v.findViewById(R.id.event_list_layout);
        }
    }

    //Provide a suitable constructor
    public EventlistAdapter(Context context, ArrayList<Event_cal_listdata> data) {
        this.clist = data;
        this.mContext = context;
    }

    //Create new views (invoked by the layout manager)
    @Override
    public EventlistAdapter.ViewHolder onCreateViewHolder(ViewGroup parent, int viewType) {

        //Creating a new view
        View v = LayoutInflater.from(parent.getContext()).inflate(R.layout.custom_event, parent, false);


        //set the view's size, margins, paddings and layout parameters

        EventlistAdapter.ViewHolder vh = new EventlistAdapter.ViewHolder(v);
        return vh;
    }

    //Replace the contents of a view (invoked by the layout manager
    @Override
    public void onBindViewHolder(EventlistAdapter.ViewHolder holder, int position) {

        // - get element from arraylist at this position
        // - replace the contents of the view with that element

        final Event_cal_listdata data = clist.get(position);

        // Format the date before setting it to the TextView
        String formattedDate = EventActivity.formatDate(data.getEvent_date());
        holder.tv_event_date.setText(formattedDate);
        holder.tv_event_name.setText(data.getEvent_name());
//        holder.event_desc.setText(data.getEvent_desc());


        // Load image using Picasso

        Picasso.get()
                .load(data.getEvent_imgurl())
                .placeholder(R.drawable.image_not_found)
                .error(R.drawable.image_not_found)
                .into(holder.news_images, new Callback() {
                    @Override
                    public void onSuccess() {
                        Log.d("Picasso", "Image loaded successfully");
                    }

                    @Override
                    public void onError(Exception e) {
                        Log.e("Picasso", "Error loading image: " + e.getMessage());
                        e.printStackTrace();
                    }
                });



        


    }

    @Override
    public int getItemCount() {
        return clist.size();
    }
}
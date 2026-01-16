package com.app.Regional_News.adapter;

import android.app.AlertDialog;
import android.content.Context;
import android.content.Intent;
import android.net.Uri;
import android.text.SpannableString;
import android.text.Spanned;
import android.text.TextPaint;
import android.text.style.ClickableSpan;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.LinearLayout;
import android.widget.TextView;

import androidx.cardview.widget.CardView;
import androidx.core.content.ContextCompat;
import androidx.recyclerview.widget.RecyclerView;

import com.app.Regional_News.EventActivity;
import com.app.Regional_News.R;
import com.app.Regional_News.data.Advertisement_listdata;
import com.squareup.picasso.Callback;
import com.squareup.picasso.Picasso;

import java.util.ArrayList;

public class AdvertisementAdapter extends RecyclerView.Adapter<AdvertisementAdapter.ViewHolder> {
    private final ArrayList<Advertisement_listdata> clist;
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
    public AdvertisementAdapter(Context context, ArrayList<Advertisement_listdata> data) {
        this.clist = data;
        this.mContext = context;
    }

    //Create new views (invoked by the layout manager)
    @Override
    public AdvertisementAdapter.ViewHolder onCreateViewHolder(ViewGroup parent, int viewType) {

        //Creating a new view
        View v = LayoutInflater.from(parent.getContext()).inflate(R.layout.custom_event, parent, false);


        //set the view's size, margins, paddings and layout parameters

        AdvertisementAdapter.ViewHolder vh = new AdvertisementAdapter.ViewHolder(v);
        return vh;
    }

    //Replace the contents of a view (invoked by the layout manager
    @Override
    public void onBindViewHolder(AdvertisementAdapter.ViewHolder holder, int position) {

        // - get element from arraylist at this position
        // - replace the contents of the view with that element

        final Advertisement_listdata data = clist.get(position);

        // Format the date before setting it to the TextView
        String formattedDate = EventActivity.formatDate(data.getAdv_date());
        holder.tv_event_date.setText(formattedDate);
        holder.tv_event_name.setText(data.getAdv_name());
//        holder.event_desc.setText(data.getEvent_desc());


        // Load image using Picasso

        Picasso.get()
                .load(data.getAdv_img())
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

        // Set click listener on the cardview to display the dialog box
        holder.event_list_layout.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String eventDetails = mContext.getString(R.string.eventdate) + formattedDate + "\n\n" +
                        mContext.getString(R.string.eventname) + data.getAdv_name() + "\n\n" +
                        mContext.getString(R.string.eventdesc) + "\n" + data.getAdv_desc() +  "\n\n" +
                        mContext.getString(R.string.eventapplyweblink)  + " " ;


                // Make the event web link clickable
                SpannableString spannableString = new SpannableString(eventDetails + mContext.getString(R.string.eventlink));
                ClickableSpan clickableSpan = new ClickableSpan() {
                    @Override
                    public void onClick(View textView) {
                        // Open the web link in the browser
                        Intent browserIntent = new Intent(Intent.ACTION_VIEW, Uri.parse(data.getAdv_weblink()));
                        mContext.startActivity(browserIntent);
                    }

                    @Override
                    public void updateDrawState(TextPaint ds) {
                        super.updateDrawState(ds);
                        int linkColor = ContextCompat.getColor(mContext, R.color.text_black_color);

                        ds.setColor(linkColor); // Set the color of the link text
                        ds.setUnderlineText(false); // Set underline
                    }
                };
                spannableString.setSpan(clickableSpan, eventDetails.length(), spannableString.length(), Spanned.SPAN_EXCLUSIVE_EXCLUSIVE);

                AlertDialog dialog = new AlertDialog.Builder(mContext)
                        .setTitle(mContext.getString(R.string.eventdialogboxtitle))
                        .setMessage(spannableString)
                        .setPositiveButton(android.R.string.ok, null)
                        .show();

                // Make the link clickable
                ((android.widget.TextView) dialog.findViewById(android.R.id.message)).setMovementMethod(android.text.method.LinkMovementMethod.getInstance());

//
//                // Create an AlertDialog to show the event name
//                AlertDialog.Builder builder = new AlertDialog.Builder(mContext);
//                builder.setTitle("Event Details");
//                builder.setMessage(data.getEvent_name());
//                builder.setPositiveButton(android.R.string.ok, null);
//
//                // Display the dialog
//                AlertDialog dialog = builder.create();
//                dialog.show();
            }
        });

    }

    @Override
    public int getItemCount() {
        return clist.size();
    }
}
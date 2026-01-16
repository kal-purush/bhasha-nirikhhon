package com.app.Regional_News.adapter;

import android.content.Context;
import android.content.Intent;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.LinearLayout;
import android.widget.TextView;

import androidx.cardview.widget.CardView;
import androidx.recyclerview.widget.RecyclerView;

import com.app.Regional_News.R;
import com.app.Regional_News.data.Scholarship_listfetch_listdata;
import com.app.Regional_News.ui.NewsShowActivity;
import com.squareup.picasso.Callback;
import com.squareup.picasso.Picasso;

import java.util.ArrayList;

public class ScholarshiplistAdapter extends RecyclerView.Adapter<ScholarshiplistAdapter.ViewHolder> {

    private final ArrayList<Scholarship_listfetch_listdata> clist;
    private final Context mContext;

    // Provide a reference to the views for each data item
    public static class ViewHolder extends RecyclerView.ViewHolder {
        public TextView tv_scholarship_name, keywordtext,s_date_from,s_date_to,s_status;
        public ImageView s_images;
        public LinearLayout Scholarship_list_layout;

        public ViewHolder(View v) {
            super(v);
            tv_scholarship_name = v.findViewById(R.id.tv_scholarship_name);
            s_images = v.findViewById(R.id.s_images); // This is where s_images ImageView is initialized
            Scholarship_list_layout = v.findViewById(R.id.Scholarship_list_layout);
            keywordtext = v.findViewById(R.id.keywordtext);
            s_date_from = v.findViewById(R.id.s_date_from);
            s_date_to = v.findViewById(R.id.s_date_to);
            s_status = v.findViewById(R.id.s_status);
        }
    }

    // Provide a suitable constructor
    public ScholarshiplistAdapter(Context context, ArrayList<Scholarship_listfetch_listdata> data) {
        this.clist = data;
        this.mContext = context;
    }

    // Create new views (invoked by the layout manager)
    @Override
    public ScholarshiplistAdapter.ViewHolder onCreateViewHolder(ViewGroup parent, int viewType) {
        // Creating a new view
        View v = LayoutInflater.from(parent.getContext()).inflate(R.layout.custom_scholarship, parent, false);
        return new ScholarshiplistAdapter.ViewHolder(v);
    }

    // Replace the contents of a view (invoked by the layout manager)
    @Override
    public void onBindViewHolder(ScholarshiplistAdapter.ViewHolder holder, int position) {
        // Get element from arraylist at this position and replace the contents of the view with that element
        final Scholarship_listfetch_listdata data = clist.get(position);
        holder.tv_scholarship_name.setText(data.getScholarship_name());
        holder.keywordtext.setText(data.getKeyword());
        holder.s_date_from.setText(data.getS_date_from());
        holder.s_date_to.setText(data.getS_date_to());
        holder.s_status.setText(data.getScholarship_status_word());


        // Set text color based on the status value
        if ("Active".equalsIgnoreCase(data.getScholarship_status_word())) {
            holder.s_status.setTextColor(mContext.getResources().getColor(R.color.green));
        } else {
            holder.s_status.setTextColor(mContext.getResources().getColor(R.color.red));
        }


        // Load image using Picasso
        Picasso.get()
                .load(data.getS_imgurl())
                .placeholder(R.drawable.image_not_found)
                .error(R.drawable.image_not_found)
                .into(holder.s_images, new Callback() {
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

        holder.Scholarship_list_layout.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent i = new Intent(mContext, NewsShowActivity.class);
                i.putExtra("mm_id", data.getScholarship_id());
                i.putExtra("mm_m_year", data.getScholarship_name());
                i.putExtra("getNews_id", data.getScholarship_id());
                i.putExtra("news_imgurl", data.getS_imgurl());

                mContext.startActivity(i);
            }
        });
    }

    @Override
    public int getItemCount() {
        return clist.size();
    }
}
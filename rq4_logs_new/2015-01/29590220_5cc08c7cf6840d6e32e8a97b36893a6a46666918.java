package com.metafour.cwbay.adapter;

import android.content.Context;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.BaseAdapter;
import android.widget.ImageView;
import android.widget.TextView;

import com.metafour.cwbay.R;
import com.metafour.cwbay.model.Ad;
import com.metafour.cwbay.model.Category;
import com.metafour.cwbay.util.Constants;

import java.util.List;

/**
 * Created by Noor on 1/29/2015.
 */
public class ProdcutListAdapter extends BaseAdapter {
    private Context mContext;
    List<Ad> items;
    private static LayoutInflater inflater = null;

    public ProdcutListAdapter(Context c, List<Ad> items) {
        mContext = c;
        this.items = items;
        try {
            inflater = (LayoutInflater) mContext.getSystemService(Context.LAYOUT_INFLATER_SERVICE);
        } catch (Exception e) {
            Log.i(Constants.ACTIVITY_LOG_TAG, "Failed to inflate.", e);
        }
    }

    public int getCount() {
        return items.size();
    }

    public Ad getItem(int position) {
        return items.get(position);
    }

    public long getItemId(int position) {
        return 0;
    }

    public static class ViewHolder1 {
        public ImageView prodImg;
        public TextView prodFirst;
        public TextView prodTitle;
        public TextView prodPrice;

    }

    // create a new ImageView for each item referenced by the Adapter
    public View getView(int position, View convertView, ViewGroup parent) {
        View vi = convertView;
        final ViewHolder1 holder;
        try {
            if (convertView == null) {
                vi = inflater.inflate(R.layout.product_list_items, null);
                holder = new ViewHolder1();
                holder.prodImg = (ImageView) vi.findViewById(R.id.prodImg);
                holder.prodFirst = (TextView) vi.findViewById(R.id.prodFirst);
                holder.prodTitle = (TextView) vi.findViewById(R.id.prodTitle);
                holder.prodPrice = (TextView) vi.findViewById(R.id.prodPrice);
                vi.setTag(holder);
            } else {
                holder = (ViewHolder1) vi.getTag();
            }
//            holder.prodImg.set(items.get(position).getName());
            holder.prodFirst.setText(items.get(position).getPostedFrom());
            holder.prodTitle.setText(items.get(position).getTitle());
            holder.prodPrice.setText(String.valueOf(items.get(position).getPrice()));
        } catch (Exception e) {
            Log.i(Constants.ACTIVITY_LOG_TAG, "Failed to list the product items.", e);
        }
        return vi;
    }
}
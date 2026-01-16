package com.freud.mrzz.adapter;


import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import android.content.Context;
import android.util.Log;
import android.util.SparseArray;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ArrayAdapter;
import android.widget.Button;
import android.widget.ImageView;
import android.widget.TextView;
import android.widget.Toast;

import com.freud.mrzz.R;
import com.freud.mrzz.utils.ImageLoader;
import com.freud.mrzz.utils.L;

/***
 * ADAPTER
 */

public class ClothListAdapter extends ArrayAdapter<String> {


    static class ViewHolder {
        TextView tv_cloth;
        ImageView iv_cloth;
    }

    private final LayoutInflater mLayoutInflater;

    private List<String> urls;
    private Context context;

    public ClothListAdapter(final Context context, final int textViewResourceId,List<String> urls) {
        super(context, textViewResourceId);
        mLayoutInflater = LayoutInflater.from(context);
        this.urls=urls;
        this.context=context;
    }

    public void addMore(List<String> urls)
    {
        this.urls.addAll(urls);
        notifyDataSetChanged();
    }

    @Override
    public View getView(final int position, View convertView, final ViewGroup parent) {

        Log.i("wang","getview   aaaaaaaaaaaaaaaaa");
        ViewHolder vh;
        if (convertView == null) {
            convertView = mLayoutInflater.inflate(R.layout.view_cloth_item, parent, false);
            vh = new ViewHolder();
            vh.iv_cloth=(ImageView) convertView.findViewById(R.id.iv_cloth);
            vh.tv_cloth=(TextView) convertView.findViewById(R.id.tv_cloth_name);

            convertView.setTag(vh);
        }
        else {
            vh = (ViewHolder) convertView.getTag();
        }

        vh.tv_cloth.setText("   "+position);
        ImageLoader.getInstance(context).DisplayImage(urls.get(position),vh.iv_cloth);
        vh.iv_cloth.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                ((ImageView)v).setImageResource(R.drawable.btn_shoes);
                Log.d("wang","aaaaaaaaaaaaaaaa     "+position);
            }
        });

        return convertView;
    }

    @Override
    public int getCount() {
        return urls.size();
    }

}
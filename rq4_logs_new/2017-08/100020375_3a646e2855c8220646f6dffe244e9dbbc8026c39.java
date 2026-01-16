package com.sandy.mvp.mvp_sample.Adapters;

import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.BaseAdapter;
import android.widget.TextView;

import com.sandy.mvp.mvp_sample.Data.SampleItem;
import com.sandy.mvp.mvp_sample.R;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by Sunil.Soni on 8/11/2017.
 */

public class SampleItemsAdapter extends BaseAdapter {


    private List<SampleItem> sampleItems;


    private void setList(List<SampleItem> sampleItems){

        this.sampleItems=checkNotNull(sampleItems);
    }
    public SampleItemsAdapter(List<SampleItem> sampleItems){
        setList(sampleItems);

    }


    public void replaceData(List<SampleItem> sampleItems){
        setList(sampleItems);
        notifyDataSetChanged();
    }

    @Override
    public int getCount() {
        return sampleItems.size();
    }

    @Override
    public SampleItem getItem(int position) {
        return sampleItems.get(position);
    }

    @Override
    public long getItemId(int position) {
        return position;
    }

    @Override
    public View getView(int position, View convertView, ViewGroup parent) {

        View rowView=convertView;
        if(rowView==null){

            LayoutInflater layoutInflater=LayoutInflater.from(parent.getContext());
            rowView=layoutInflater.inflate(R.layout.sample_list_item_layout,parent,false);
        }


        final SampleItem sampleItem=getItem(position);
        TextView textView=(TextView) rowView.findViewById(R.id.itemName);

        textView.setText(sampleItem.getItemName());

        return rowView;
    }
}
package com.dev.mobile.apteks.Adapters;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.BaseAdapter;
import android.widget.TextView;

import com.dev.mobile.apteks.Models.Drug;
import com.dev.mobile.apteks.R;


import java.util.ArrayList;


public class FindDrugsAdapter extends BaseAdapter {
    private ArrayList<Drug> listData;
    private LayoutInflater inflater;

    public void clear() {
        if(this.listData != null) {
            this.listData.clear();
        }

        this.notifyDataSetChanged();
    }

    public ArrayList<Drug> getData() {
        return this.listData;
    }

    public void updateContent(ArrayList<Drug> data) {
        this.listData = data;
        this.notifyDataSetChanged();
    }

    public FindDrugsAdapter(Context c) {
        this.inflater = LayoutInflater.from(c);
        this.listData = new ArrayList<Drug>();
    }

    @Override
    public int getCount() {
        if(this.listData == null) {
            return 0;
        }

        return this.listData.size();
    }

    @Override
    public Object getItem(int position) {
        return this.listData.get(position);
    }

    @Override
    public long getItemId(int position) {
        return position;
    }

    @Override
    public View getView(int position, View convertView, ViewGroup parent) {
        View view = convertView;

        if(view == null) {
            view = this.inflater.inflate(R.layout.drug_item, parent, false);
        }

        Drug drug = (Drug) this.getItem(position);

        ((TextView) view.findViewById(R.id.drugName)).setText(drug.getDrugName());
        ((TextView) view.findViewById(R.id.drugPrice)).setText(drug.getDrugPrice());
        ((TextView) view.findViewById(R.id.pharmacyName)).setText(drug.getPharmacyName());

        return view;
    }

    @Override
    public boolean isEmpty() {
        //return super.isEmpty();

        return false;
    }

    // small hack :-)
    public boolean isRealEmpty() {
        if(this.listData != null && this.listData.size() > 0) {
            return false;
        }

        return true;
    }
}
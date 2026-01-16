package com.kanyi.bigtuna;

import android.widget.Filter;

import com.kanyi.bigtuna.adapter.AdapterOrderShop;
import com.kanyi.bigtuna.adapter.AdapterProductSeller;
import com.kanyi.bigtuna.models.ModelOrderShop;
import com.kanyi.bigtuna.models.ModelProduct;

import java.util.ArrayList;

public class FilterOrderShop extends Filter {

    private AdapterOrderShop adapter;
    private ArrayList<ModelOrderShop> filterList;

    public FilterOrderShop(AdapterOrderShop adapter , ArrayList < ModelOrderShop > filterList) {
        this.adapter = adapter;
        this.filterList = filterList;
    }

    @Override
    protected FilterResults performFiltering(CharSequence constraint) {
        FilterResults results = new FilterResults ();
        // validate data for search query
        if(constraint != null && constraint.length () >0 ){
            // search field not empty , search something, perform search
            // change to uppercase to make case insensitive
            constraint = constraint.toString ().toUpperCase();
            // store our filtered list
            ArrayList <ModelOrderShop>  filteredModels = new ArrayList <> (  );
            for (int i=0; i< filterList.size ( ); i++){
                // check, search by title or category
                if(filterList.get ( i ).getOrderStatus ().toUpperCase ().contains ( constraint )){

                    // add filtered data to list
                    filteredModels.add(filterList.get ( i ));
                }
            }
            results.count = filteredModels.size ();
            results.values= filteredModels;
        }
        else {
            // search field empty , not searching , return original/complete list

            results.count = filterList.size ();
            results.values= filterList;
        }
        return results;
    }

    @Override
    protected void publishResults(CharSequence constraint , FilterResults results) {
        adapter.orderShopArrayList= (ArrayList< ModelOrderShop>) results.values;
        // refresh adapter
        adapter.notifyDataSetChanged ();
    }
}
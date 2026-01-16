package com.kanyi.bigtuna;

import android.widget.Filter;

import com.kanyi.bigtuna.adapter.AdapterProductSeller;
import com.kanyi.bigtuna.adapter.AdapterProductUser;
import com.kanyi.bigtuna.models.ModelProduct;

import java.util.ArrayList;

public class FilterProductUser extends Filter {

    private AdapterProductUser adapter;
    private ArrayList<ModelProduct> filterList;

    public FilterProductUser(AdapterProductUser adapter , ArrayList < ModelProduct > filterList) {
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
            ArrayList <ModelProduct>  filteredModels = new ArrayList <> (  );
            for (int i=0; i< filterList.size ( ); i++){
                // check, search by title or category
                if(filterList.get ( i ).getProductTitle ().toUpperCase ().contains ( constraint ) ||
                        filterList.get ( i ).getProductCategory ().toUpperCase ().contains ( constraint )
                ) {
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
        adapter.productsList = (ArrayList< ModelProduct>) results.values;
        // refresh adapter
        adapter.notifyDataSetChanged ();
    }
}
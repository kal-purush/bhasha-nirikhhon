package com.dev.mobile.apteks.Tasks;

import android.content.Context;
import android.os.AsyncTask;
import android.view.View;

import com.dev.mobile.apteks.Adapters.FindDrugsAdapter;
import com.dev.mobile.apteks.FindDrugsActivity;
import com.dev.mobile.apteks.MainActivity;
import com.dev.mobile.apteks.Models.Drug;
import com.dev.mobile.apteks.R;

import java.util.ArrayList;


public class LoadPageDrugTask extends AsyncTask<Void, Void, ArrayList<Drug>> {
    private View view;
    private String searchString;
    private FindDrugsAdapter adapter;
    private Context context;

    public LoadPageDrugTask(View v, FindDrugsAdapter a, String search, Context c) {
        this.view = v;
        this.adapter = a;
        this.searchString = search;
        this.context = c;
    }

    @Override
    protected void onPreExecute() {
        super.onPreExecute();
    }

    @Override
    protected ArrayList<Drug> doInBackground(Void... params) {
        try {
            return Drug.findDrug(this.searchString, this.adapter);
        }catch(Exception ex) {                  // TODO catch exceptions

        }
        return null;
    }



    @Override
    protected void onPostExecute(ArrayList<Drug> data) {
        super.onPostExecute(data);

        this.adapter.updateContent(data);

        ((FindDrugsActivity)this.context).resetLoadingFlag();
        this.view.findViewById(R.id.loadMoreFrameLayout).setVisibility(View.GONE);
    }
}
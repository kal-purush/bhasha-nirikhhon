package com.metafour.cwbay.ui;

import android.app.Activity;
import android.content.Context;
import android.content.Intent;
import android.os.Bundle;
import android.support.v7.app.ActionBarActivity;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.AdapterView;
import android.widget.ArrayAdapter;
import android.widget.ListView;
import android.widget.TextView;

import com.metafour.cwbay.R;
import com.metafour.cwbay.model.Category;
import com.metafour.cwbay.remote.WebAPI;
import com.metafour.cwbay.util.Constants;
import com.metafour.cwbay.util.Utility;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Noor on 1/27/2015.
 */
public class CategoryActivity extends ActionBarActivity {
    public static int idToShow;
    private Context context;
    private ListView catLItems;
    private TextView catLPath;
    private Category category;
    public static String path = ">>";
    private String prevPath;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.category);

        this.context = getApplicationContext();
        this.catLItems = (ListView) findViewById(R.id.catLItems);
        this.catLPath = (TextView) findViewById(R.id.catLPath);

        WebAPI.categoryView(this.context, new WebAPI.Callback<Category>(){
            @Override
            public void onFailed(String reason) {
                Log.i(Constants.ACTIVITY_LOG_TAG, reason);
                Utility.showShortLengthToast(context, reason);
            }
            @Override
            public void onSuccess(Category category1) {
                Log.i(Constants.ACTIVITY_LOG_TAG, "Category list received successfully. " + category1.toString());
                category = category1;
                showCategories();
            }
        }, idToShow);

//        category = getDummyCategory();
        catLItems.setOnItemClickListener(new AdapterView.OnItemClickListener() {
            @Override
            public void onItemClick(AdapterView parent, final View view,
                                    int position, long id) {
                final Category category = (Category) parent.getItemAtPosition(position);
                if (category.isHasChildren()) {
                    CategoryActivity.idToShow = category.getId();
                    showNextCategoryActivity();
                } else {
                    Utility.showShortLengthToast(context, "Product list page is not implemented yet");
                    // showProductListPage();
                }
            }

        });
    }

    @Override
    public void onBackPressed() {
        super.onBackPressed();
        Utility.backWithAnimation(this);
        path = prevPath;
    }

    private void showCategories() {
        catLItems.setAdapter(new ArrayAdapter<Category>(this, android.R.layout.simple_list_item_1, android.R.id.text1, category.getChildren()) {
            @Override
            public View getView(int position, View convertView, ViewGroup parent) {
                View view = super.getView(position, convertView, parent);
                TextView text1 = (TextView) view.findViewById(android.R.id.text1);
                text1.setText(getItem(position).getName());
                return view;
            }
        });

        if (idToShow > 0) {
            prevPath = path;
            path += (path.length() > 2 ? ">" : "") + category.getName();
            catLPath.setText(path);
        } else {
            catLPath.setVisibility(View.INVISIBLE);
            prevPath = ">>";
        }
    }

    private void showNextCategoryActivity () {
        Log.i(Constants.ACTIVITY_LOG_TAG, "Going to open category page for id = " + idToShow);
        startActivity(new Intent(this, CategoryActivity.class));
        Utility.nextWithAnimation(this);
    }

    private Category getDummyCategory() {
        Category cat = new Category();
        cat.setName("Test");
        List<Category> children = new ArrayList<Category>();
        for (int i = 0; i < 24; i++) {
            Category child = new Category();
            child.setId(i + 1);
            child.setName("Test " + child.getId());
            child.setHasChildren(true);
            children.add(child);
        }
        cat.setChildren(children);
        return cat;
    }
}
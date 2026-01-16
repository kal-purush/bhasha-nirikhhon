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
import android.widget.BaseAdapter;
import android.widget.GridView;
import android.widget.ListView;
import android.widget.TextView;

import com.metafour.cwbay.R;
import com.metafour.cwbay.model.Category;
import com.metafour.cwbay.util.Constants;
import com.metafour.cwbay.util.Utility;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Noor on 1/27/2015.
 */
public class CategoryGridActivity extends ActionBarActivity {
    public static int idToShow;
    private Context context;
    private GridView catGItems;
    private Map<String, Category> textCatMap;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.category_grid);

        this.context = getApplicationContext();
        this.catGItems = (GridView) findViewById(R.id.catGItems);
        this.textCatMap = new HashMap<String, Category>();

        populateCategoryList(getDummyCategory());

        catGItems.setOnItemClickListener(new AdapterView.OnItemClickListener() {
            @Override
            public void onItemClick(AdapterView<?> parent, final View view,
                                    int position, long id) {
                Category category = (Category) parent.getItemAtPosition(position);
                if (category.isHasChildren()) {
                    CategoryGridActivity.idToShow = category.getId();
                    showNextCategoryActivity();
                } else {
                    Utility.showShortLengthToast(context, "Product list page is not implemented yet");
                    // showProductListPage();
                }
            }

        });
    }

    private void populateCategoryList(Category category) {
        final List<String> items = new ArrayList<String>();
        for (Category child : category.getChildren()) {
            items.add(child.getName());
            textCatMap.put(child.getName(), child);
        }
        catGItems.setAdapter(new ImageAdapter(this, category.getChildren()));
//        catGItems.setAdapter(adapter);
    }

    private void showNextCategoryActivity () {
        Log.i(Constants.ACTIVITY_LOG_TAG, "Going to open category page for id = " + idToShow);
        startActivity(new Intent(this, CategoryGridActivity.class));
    }

    private Category getDummyCategory() {
        Category cat = new Category();
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

    public static class ImageAdapter extends BaseAdapter {
        private Context mContext;
        List<Category> items;
        private static LayoutInflater inflater = null;

        public ImageAdapter(Context c, List<Category> items) {
            mContext = c;
            this.items = items;
            try {
                inflater = (LayoutInflater) mContext.getSystemService(Context.LAYOUT_INFLATER_SERVICE);

            } catch (Exception e) {

            }
        }

        public int getCount() {
            return items.size();
        }

        public Category getItem(int position) {
            return items.get(position);
        }

        public long getItemId(int position) {
            return 0;
        }

        public static class ViewHolder1 {
            public TextView display_name;
            public TextView display_number;

        }

        // create a new ImageView for each item referenced by the Adapter
        public View getView(int position, View convertView, ViewGroup parent) {

           /* TextView imageView;
            if (convertView == null) {  // if it's not recycled, initialize some attributes
                imageView = new TextView(mContext);
                imageView.setLayoutParams(new GridView.LayoutParams(85, 85));
                imageView.setPadding(8, 8, 8, 8);
            } else {
                imageView = (TextView) convertView;
            }

            imageView.setText(getItem(position).getName());*/

            View vi = convertView;
            final ViewHolder1 holder;
            try {
                if (convertView == null) {
                    vi = inflater.inflate(R.layout.category_two_lines, null);
                    holder = new ViewHolder1();

                    holder.display_name = (TextView) vi.findViewById(R.id.display_name);
                    holder.display_number = (TextView) vi.findViewById(R.id.display_number);


                    vi.setTag(holder);
                } else {
                    holder = (ViewHolder1) vi.getTag();
                }



                holder.display_name.setText(items.get(position).getName());
                holder.display_number.setText(items.get(position).getName());


            } catch (Exception e) {


            }
            return vi;
        }
    }
}
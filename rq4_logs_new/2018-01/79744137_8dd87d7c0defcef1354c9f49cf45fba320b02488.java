package com.example.sudhakar.vocabcards;

import android.content.Intent;
import android.support.design.widget.Snackbar;
import android.support.v4.widget.DrawerLayout;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.support.v7.widget.Toolbar;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.AdapterView;
import android.widget.ArrayAdapter;
import android.widget.LinearLayout;
import android.widget.ListView;
import android.widget.ScrollView;
import android.widget.TextView;

public class RevisionHome extends AppCompatActivity {

    public static final String EXTRA_MESSAGE = "Revision.MESSAGE";
    /*
    For the settings drawer
     */
    private String[] mPlanetTitles = {"Home", "Mercury", "Earth", "Mars"};
    private DrawerLayout mDrawerLayout;
    private ListView mDrawerList;


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_revision_home);


        mDrawerLayout = (DrawerLayout) findViewById(R.id.revision_home_drawer_layout);
        mDrawerList = (ListView) findViewById(R.id.revision_home_drawer);

        // Set the adapter for the list view
        mDrawerList.setAdapter(new ArrayAdapter<String>(this,
                R.layout.drawer_list_item, mPlanetTitles));
        // Set the list's click listener
        mDrawerList.setOnItemClickListener(new RevisionHome.DrawerItemClickListener());

        ScrollView sv = (ScrollView) findViewById(R.id.revision_home_scroll_view);


        LinearLayout ll = (LinearLayout) LayoutInflater.from(getApplicationContext())
                .inflate(R.layout.list_custom_layout,null);

        ((TextView) ll.getChildAt(0)).setText("This");
                //(LinearLayout) LayoutInflater.from(parent.getContext())
                //.inflate(R.layout.list_custom_layout, parent, false);
        sv.addView(ll);

        setContentView(R.layout.activity_revision_home);

    }

    private class DrawerItemClickListener implements ListView.OnItemClickListener {
        @Override
        public void onItemClick(AdapterView<?> parent, View view, int position, long id) {
            String ret = selectItem(position);


            if(ret.equalsIgnoreCase("Home"))
                StartHomeActivity();


            Snackbar.make(view, ret, Snackbar.LENGTH_LONG)
                    .setAction("Action", null).show();
        }
    }

    /** Swaps fragments in the main content view */
    private String selectItem(int position) {
        // Highlight the selected item, update the title, and close the drawer
        mDrawerList.setItemChecked(position, true);
        //setTitle(mPlanetTitles[position]);
        mDrawerLayout.closeDrawer(mDrawerList);
        return mPlanetTitles[position];
    }


    private void StartHomeActivity(){
        Intent intent = new Intent(this, MainActivity.class);
        String message = "Dummy";
        intent.putExtra(EXTRA_MESSAGE, message);
        startActivity(intent);
    }
}
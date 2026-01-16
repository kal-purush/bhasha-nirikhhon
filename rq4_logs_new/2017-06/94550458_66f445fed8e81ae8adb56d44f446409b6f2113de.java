package org.jamaica.pcog.mobile.resources;

import android.content.Context;
import android.content.Intent;
import android.net.Uri;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.support.v7.widget.Toolbar;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.AdapterView;
import android.widget.BaseAdapter;
import android.widget.ImageView;
import android.widget.ListView;
import android.widget.TextView;
import android.widget.Toast;

import com.bumptech.glide.Glide;

import org.jamaica.pcog.mobile.ContactActivity;
import org.jamaica.pcog.mobile.MainActivity;
import org.jamaica.pcog.mobile.R;
import org.jamaica.pcog.mobile.profile.ProfileModelHome;

import java.util.ArrayList;

public class Resources extends AppCompatActivity {

    private ListView lvResources;
    private MyAppAdapter myAppAdapter;
    private ArrayList<ProfileModelHome> profileModelArrayList;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_resources);
        Toolbar toolbar = (Toolbar) findViewById(R.id.toolbar);
        setSupportActionBar(toolbar);

        profileModelArrayList = new ArrayList<>();
        profileModelArrayList.add(new ProfileModelHome("http://www.google.com ", "", R.drawable.phoneee));
        profileModelArrayList.add(new ProfileModelHome("https://nodejs.org/en/", "", R.drawable.phoneee));
        profileModelArrayList.add(new ProfileModelHome("https://www.teamviewer.com/en/", "", R.drawable.webicon));
        profileModelArrayList.add(new ProfileModelHome("Email Us ", "portmorechurchogod@gmail.com", R.drawable.emaile));

        lvResources = (ListView) findViewById(R.id.lblist);
        myAppAdapter = new MyAppAdapter(profileModelArrayList, getApplicationContext());
        lvResources.setAdapter(myAppAdapter);
        lvResources.setOnItemClickListener(new ItemList());

        // Toolbar
        toolbar = (Toolbar) findViewById(R.id.toolbar);
        setSupportActionBar(toolbar);

        // add back arrow to toolbar
        if (getSupportActionBar() != null) {
            getSupportActionBar().setDisplayHomeAsUpEnabled(true);
            getSupportActionBar().setDisplayShowHomeEnabled(true);
        }

        toolbar.setNavigationOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent intent = new Intent(getApplicationContext(), MainActivity.class);
                startActivity(intent);
                finish();
            }

        });

        getSupportActionBar().setTitle("Contact Us");
    }


    public class MyAppAdapter extends BaseAdapter {

        public class ViewHolder {
            TextView username, country;
            ImageView profilePic;

        }

        public ArrayList<ProfileModelHome> profileList;

        public Context context;


        private MyAppAdapter(ArrayList<ProfileModelHome> apps, Context context) {
            this.profileList = apps;
            this.context = context;

        }

        @Override
        public int getCount() {
            return profileList.size();
        }

        @Override
        public Object getItem(int position) {
            return position;
        }

        @Override
        public long getItemId(int position) {
            return position;
        }

        @Override
        public View getView(final int position, View convertView, ViewGroup parent) {

            View rowView = convertView;
            MyAppAdapter.ViewHolder viewHolder;

            if (rowView == null) {
                LayoutInflater inflater = getLayoutInflater();
                rowView = inflater.inflate(R.layout.list_single, parent, false);

                viewHolder = new MyAppAdapter.ViewHolder();
                viewHolder.profilePic = (ImageView) rowView.findViewById(R.id.imgProfile);
                viewHolder.username = (TextView) rowView.findViewById(R.id.txtUsername);
                viewHolder.country = (TextView) rowView.findViewById(R.id.txtCountry);
                rowView.setTag(viewHolder);

            } else {
                viewHolder = (MyAppAdapter.ViewHolder) convertView.getTag();
            }

            viewHolder.username.setText(profileList.get(position).getUsername() + "");
            viewHolder.country.setText(profileList.get(position).getCountry() + "");
            Glide.with(getApplicationContext()).load(profileList.get(position).getProfilePic()).into(viewHolder.profilePic);

            return rowView;
        }
    }

    private class ItemList implements AdapterView.OnItemClickListener {

        @Override
        public void onItemClick(AdapterView<?> adapterView, View view, int i, long l) {


            if (i == 0) {

                Toast.makeText(getApplicationContext(),"sumthn",Toast.LENGTH_SHORT).show();
            }

            else if (i == 1) {
                Toast.makeText(getApplicationContext(),"sumthn",Toast.LENGTH_SHORT).show();

            }
            else if (i == 2) {
                Toast.makeText(getApplicationContext(),"sumthn",Toast.LENGTH_SHORT).show();

            }
            else if (i == 3) {
                Toast.makeText(getApplicationContext(),"sumthn",Toast.LENGTH_SHORT).show();

            }
            else if (i == 4) {
                Toast.makeText(getApplicationContext(),"sumthn",Toast.LENGTH_SHORT).show();
            }

        }
    }

    @Override
    public void onBackPressed() {
        Intent intent = new Intent(getApplicationContext(), MainActivity.class);
        startActivity(intent);
        overridePendingTransition(R.anim.fade_in, R.anim.fade_out);
        this.finish();
    }
}
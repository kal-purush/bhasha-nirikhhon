package com.example.tesla.u_smart;

import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.os.AsyncTask;
import android.os.Bundle;
import android.support.design.widget.FloatingActionButton;
import android.support.design.widget.Snackbar;
import android.support.v7.widget.LinearLayoutManager;
import android.support.v7.widget.RecyclerView;
import android.util.Log;
import android.view.View;
import android.support.design.widget.NavigationView;
import android.support.v4.view.GravityCompat;
import android.support.v4.widget.DrawerLayout;
import android.support.v7.app.ActionBarDrawerToggle;
import android.support.v7.app.AppCompatActivity;
import android.support.v7.widget.Toolbar;
import android.view.Menu;
import android.view.MenuItem;
import android.view.ViewGroup;
import android.widget.ArrayAdapter;
import android.widget.ImageView;
import android.widget.ListAdapter;
import android.widget.ListView;
import android.widget.SimpleAdapter;
import android.widget.Toast;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;

public class TeacherNav extends AppCompatActivity
        implements NavigationView.OnNavigationItemSelectedListener {
ListView lv;
ImageView headerImage;
    Bitmap bitmap;
    ArrayList<HashMap<String, String>> productsList=new ArrayList<HashMap<String, String>>();
    JSONArray  array=null;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_teacher_nav);
        Toolbar toolbar = (Toolbar) findViewById(R.id.toolbar);
        setSupportActionBar(toolbar);

        FloatingActionButton fab = (FloatingActionButton) findViewById(R.id.fab);
        fab.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                Snackbar.make(view, "Replace with your own action", Snackbar.LENGTH_LONG)
                        .setAction("Action", null).show();
            }
        });

        DrawerLayout drawer = (DrawerLayout) findViewById(R.id.drawer_layout);
        ActionBarDrawerToggle toggle = new ActionBarDrawerToggle(
                this, drawer, toolbar, R.string.navigation_drawer_open, R.string.navigation_drawer_close);
        drawer.setDrawerListener(toggle);
        toggle.syncState();

        NavigationView navigationView = (NavigationView) findViewById(R.id.nav_view);
        navigationView.setNavigationItemSelectedListener(this);
      lv  =  (ListView)findViewById(R.id.lessonlistView);

getData();
    }

    @Override
    public void onBackPressed() {
        DrawerLayout drawer = (DrawerLayout) findViewById(R.id.drawer_layout);
        if (drawer.isDrawerOpen(GravityCompat.START)) {
            drawer.closeDrawer(GravityCompat.START);
        } else {
            super.onBackPressed();
        }
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.teacher_nav, menu);
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        // Handle action bar item clicks here. The action bar will
        // automatically handle clicks on the Home/Up button, so long
        // as you specify a parent activity in AndroidManifest.xml.
        int id = item.getItemId();

        //noinspection SimplifiableIfStatement
        if (id == R.id.action_settings) {
            return true;
        }

        return super.onOptionsItemSelected(item);
    }

    @SuppressWarnings("StatementWithEmptyBody")
    @Override
    public boolean onNavigationItemSelected(MenuItem item) {
        // Handle navigation view item clicks here.
        int id = item.getItemId();

        if (id == R.id.nav_camara) {
            // Handle the camera action
        } else if (id == R.id.nav_gallery) {

        } else if (id == R.id.nav_slideshow) {

        } else if (id == R.id.nav_manage) {

        } else if (id == R.id.nav_share) {

        } else if (id == R.id.nav_send) {

        }

        DrawerLayout drawer = (DrawerLayout) findViewById(R.id.drawer_layout);
        drawer.closeDrawer(GravityCompat.START);
        return true;
    }
    public void  getData(){
        class  Server extends AsyncTask<String,Void,String> {
            private String tname="hicheel";
            private  String lcode="hicheeliin_code";
            private  String lname="hicheeliin_ner";
            @Override
            protected String doInBackground(String... params) {
                try {
                    JSONObject jsonObject = make();
                    array =jsonObject.getJSONArray(tname);
                    for (int  n=0;n<array.length();n++){
                        JSONObject c=array.getJSONObject(n);
                        String lh  = c.getString(lcode);
                        String ln=c.getString(lname);
                        HashMap<String,String> map  =  new HashMap<String,String>();
                        map.put(lcode,lh);
                        map.put(lname,ln);
                        productsList.add(map);
                    }

                } catch (JSONException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            protected void onPostExecute(String s) {
                ListAdapter listAdapter = new SimpleAdapter(TeacherNav.this,productsList,R.layout.list_item,new String[]{lcode,lname},new int[]{R.id.lcode,R.id.lname});
                lv.setAdapter(listAdapter);
            }
        }
   Server  s  =  new Server();
        s.execute();
    }
    public JSONObject make(){
        InputStream st=null;
        JSONObject  object = null;
        String  res  ="";
        try{
            HttpClient client = new DefaultHttpClient();
            HttpPost post  =  new HttpPost("http://10.2.201.4/config/hicheel.php");

            HttpResponse response = client.execute(post);
            HttpEntity entity = response.getEntity();
            st   =  entity.getContent();
        }catch (Exception e){

            Log.e(" not Connect", "not  connect");
        }
        try{
            BufferedReader reader   =  new BufferedReader(new InputStreamReader(st,"UTF-8"),8);
            StringBuilder sb =  new StringBuilder();
            String line  ="";
            while((line =reader.readLine())!=null){
                sb.append(line + "\n");
            }
            st.close();
            res = sb.toString();
        }catch (Exception e1) {
            Log.e("Exception","Exception");
        }
        try {
            object = new JSONObject(res);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return  object;
    }
public  void  loginImage(String urlx){
headerImage  =  (ImageView)findViewById(R.id.headerImageView);
 new LoadImage().execute(urlx);
}
     class LoadImage extends AsyncTask<String, String, Bitmap> {

        protected Bitmap doInBackground(String... args) {
            try {
                bitmap = BitmapFactory.decodeStream((InputStream) new URL(args[0]).getContent());

            } catch (Exception e) {
                e.printStackTrace();
            }
            return bitmap;
        }

        protected void onPostExecute(Bitmap image) {

            if(image != null){
                headerImage.setImageBitmap(image);


            }else{
                Toast.makeText(TeacherNav.this, "Image Does Not exist or Network Error", Toast.LENGTH_SHORT).show();
            }
        }
    }

}
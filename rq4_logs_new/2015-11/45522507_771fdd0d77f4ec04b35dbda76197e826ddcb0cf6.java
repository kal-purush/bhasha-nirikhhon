package com.example.tesla.u_smart.Entity;

import android.app.Activity;
import android.os.AsyncTask;
import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.util.Log;
import android.widget.ListAdapter;
import android.widget.ListView;
import android.widget.SimpleAdapter;
import android.widget.Toast;

import com.example.tesla.u_smart.R;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by Sk_m4_gg on 15.11.10.
 */
public class Hicheeliin_huvaari  extends AppCompatActivity{
  public  ListView lv;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.content_teacher_nav);
lv= (ListView) findViewById(R.id.lessonlistView);
        getHicheel(lv);
    }
    public void getHicheel(ListView l){

//           Toast.makeText(getApplicationContext(),"getHicheel",Toast.LENGTH_SHORT).show();
        class Hicheel extends AsyncTask<String, Void, String> {

        ArrayList<HashMap<String, String>> arrayList = new ArrayList<HashMap<String, String>>();
        public static final String tname = "hicheeliin_huwaari";
        public static final String ls_name = "hicheeliin_ner";
        public static final String oro_h = "hicheeliin_oroh_helber";
        public static final String group = "hicheeliin_huwaariin_code";
        public static final String date = "create_date";
        JSONArray jsonArray = null;

        @Override
        protected void onPostExecute(String s) {
            try {
                ListAdapter listAdapter = new SimpleAdapter(Hicheeliin_huvaari.this, arrayList, R.layout.hicheel_layout, new String[]{ls_name, oro_h, group, date}, new int[]{R.id.lessonname, R.id.helber, R.id.group, R.id.date});
                lv.setAdapter(listAdapter);
            }
            catch (Exception r){
                Log.e("алдаа гарлаа","list" + r.getMessage());
            }}

        @Override
        protected String doInBackground(String... params) {
            JSONObject object = make("http://10.11.9.184/config/huvaari.php");
            try {
                JSONArray j = object.getJSONArray(tname);
                for (int i = 0; i < j.length(); i++) {
                    JSONObject v = j.getJSONObject(i);
                    String lname = v.getString(ls_name);
                    String or = v.getString(oro_h);
                    String g = v.getString(group);
                    String d = v.getString(date);
                    HashMap<String, String> map = new HashMap<String, String>();
                    map.put(ls_name, lname);
                    map.put(oro_h, or);
                    map.put(group, g);
                    map.put(date, d);
                    arrayList.add(map);
                }
            } catch (Exception e) {
                Log.e("Hicheeliin  huvaari ", "алдаа гарлаа " + e.getMessage());

            }
            return null;
        }
    }

    Hicheel h = new Hicheel();
    h.execute();
}

        public JSONObject make(String url) {
            InputStream st = null;
            JSONObject object = null;
            String res = "";
            try {
                HttpClient client = new DefaultHttpClient();
                HttpPost post = new HttpPost(url);

                HttpResponse response = client.execute(post);
                HttpEntity entity = response.getEntity();
                st = entity.getContent();
            } catch (Exception e) {

                Log.e(" not Connect", "not  connect");
            }
            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(st, "UTF-8"), 8);
                StringBuilder sb = new StringBuilder();
                String line = "";
                while ((line = reader.readLine()) != null) {
                    sb.append(line + "\n");
                }
                st.close();
                res = sb.toString();
            } catch (Exception e1) {
                Log.e("Exception", "Exception");
            }
            try {
                object = new JSONObject(res);
            } catch (JSONException e) {
                e.printStackTrace();
            }
            return object;
        }
    }
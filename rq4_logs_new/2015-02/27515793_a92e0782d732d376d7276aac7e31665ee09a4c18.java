package com.example.androidsro;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import android.os.AsyncTask;
import android.os.Bundle;

public class WebserviceCallActivity extends BaseActivity {

	private String goGetUsersCallURL = "http://10.0.2.2:8080/getjsonsample/";
	
	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_webservice_call);
		
		new CallUserWebserviceAPI().execute(goGetUsersCallURL);
	}
	
	private class CallUserWebserviceAPI extends AsyncTask<String, String, String> {

		@Override
		protected String doInBackground(String... params) {
			
			String inputStr = "";
			//InputStream in = null;
			
			try {
				URL url = new URL(goGetUsersCallURL);
		        
		        BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));
		        
		        JSONParser parser=new JSONParser();
		        JSONObject object = (JSONObject)parser.parse(in);
		        
		        inputStr = object.toJSONString();
		        
			} catch (IOException | ParseException e) {
				e.printStackTrace();
			}
			
			return inputStr;
		}
		
		protected void onPostExecute(String result) {
			System.out.println(result);
		}
	}
}
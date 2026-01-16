package com.example.uitest;

import android.app.Activity;
import android.os.Bundle;
import android.view.MenuItem;
import android.view.Window;

public class FrameLayoutActivity extends Activity {
	@Override
	protected void onCreate(Bundle savedInstanceState){
		super.onCreate(savedInstanceState);
		requestWindowFeature(Window.FEATURE_NO_TITLE);//hide the title;
		setContentView(R.layout.framelayout_main);
	}
	@Override
	public boolean onOptionsItemSelected(MenuItem item){
		super.onOptionsItemSelected(item);
		
		
		return true;
	}	
}

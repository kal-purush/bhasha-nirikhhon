package com.example.rockpaperscissors;

import android.os.Bundle;
import android.app.Activity;
import android.view.Menu;

public class ModesActivity extends Activity {

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_modes);
	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.activity_modes, menu);
		return true;
	}

}
package com.rubyapps.addictedtostyle.activity;

import java.util.List;

import android.app.Activity;
import android.content.Intent;
import android.os.Bundle;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.widget.AdapterView;
import android.widget.AdapterView.OnItemClickListener;
import android.widget.GridView;
import android.widget.ShareActionProvider;

import com.appodeal.ads.Appodeal;
import com.rubyapps.addictedtostyle.R;
import com.rubyapps.addictedtostyle.adapter.MyGridViewAdapter;
import com.rubyapps.addictedtostyle.app.AppConfig;
import com.rubyapps.addictedtostyle.app.MyApplication;
import com.rubyapps.addictedtostyle.helper.ParseUtils;
import com.rubyapps.addictedtostyle.model.GridItem;

public class MainActivity extends Activity {

	private GridView gridView;
	private ShareActionProvider mShareActionProvider;

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_main);
		ParseUtils.verifyParseConfiguration(this);

		gridView = (GridView) findViewById(R.id.gridView);
		final List<GridItem> itemsList = ((MyApplication) this.getApplication())
				.getItemsList();
		MyGridViewAdapter adapter = new MyGridViewAdapter(this,
				R.layout.grid_item, itemsList);
		gridView.setAdapter(adapter);
		gridView.setOnItemClickListener(new OnItemClickListener() {

			@Override
			public void onItemClick(AdapterView<?> arg0, View arg1, int arg2,
					long arg3) {
				Intent intent = new Intent(MainActivity.this,
						WebViewActivity.class);
				intent.putExtra("url", itemsList.get(arg2).getUrl());
				startActivity(intent);
			}
		});
		// android.provider.Settings.System.putInt(getContentResolver(),
		// "notification_light_pulse", 1); 1 - indicate on state 0 - indicate
		// off state
		Appodeal.disableLocationPermissionCheck();
		Appodeal.initialize(this, AppConfig.ADD_APP_KEY, Appodeal.BANNER);
		Appodeal.show(this, Appodeal.BANNER_BOTTOM);
	}

	@Override
	public void onResume() {
		super.onResume();
		Appodeal.onResume(this, Appodeal.BANNER);

		Intent intent = getIntent();
		String url = intent.getStringExtra("url");
		if (url != null && !url.isEmpty()) {
			Intent intentWeb = new Intent(MainActivity.this,
					WebViewActivity.class);
			intentWeb.putExtra("url", url);
			startActivity(intentWeb);
		}
	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.menu, menu);
		// Set up ShareActionProvider's default share intent
		MenuItem shareItem = menu.findItem(R.id.action_share);
		mShareActionProvider = (ShareActionProvider) shareItem
				.getActionProvider();
		mShareActionProvider.setShareIntent(getDefaultIntent());

		return super.onCreateOptionsMenu(menu);

	}

	private Intent getDefaultIntent() {
		Intent intent = new Intent(Intent.ACTION_SEND);
		String shareBody = "Please provide this text. https://play.google.com/store/apps/details?id=";
		intent.putExtra(android.content.Intent.EXTRA_SUBJECT,
				"Subject to provide");
		intent.putExtra(android.content.Intent.EXTRA_TEXT, shareBody
				+ getPackageName());
		intent.setType("text/plain");
		return intent;
	}

	@Override
	public boolean onOptionsItemSelected(MenuItem item) {

		return super.onOptionsItemSelected(item);
	}

	@Override
	public void onBackPressed() {
		finish();
	}
}
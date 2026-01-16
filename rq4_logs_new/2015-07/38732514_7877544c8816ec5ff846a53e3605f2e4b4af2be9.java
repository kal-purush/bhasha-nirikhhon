package com.samsung.ip;

import java.util.ArrayList;
import java.util.HashMap;

import com.samsung.ip.algorithm.AlgorithmItem;
import com.samsung.ip.algorithm.AlgorithmData;
import com.samsung.ip.algorithm.AlgorithmEnableData;
import com.samsung.ip.algorithm.AlgrithmAdapter;
import com.samsung.ip.draglist.DragSortListView;

import android.app.Activity;
import android.app.ListActivity;
import android.content.Context;
import android.content.Intent;
import android.graphics.drawable.ColorDrawable;
import android.os.Bundle;
import android.util.Log;
import android.view.KeyEvent;
import android.view.Menu;
import android.view.MenuInflater;
import android.view.MenuItem;
import android.view.WindowManager;

public class AlgorithmSettingListView extends ListActivity {

	final int REQ_CODE_PARA_SETTING = 102;
	private AlgrithmAdapter m_adapter;
	private DragSortListView list;
	private AlgorithmData algorithmData;
	private AlgorithmEnableData algorithmEnableData;
	private int algorithmCnt;

	private Context mContext;
	public static final String TAG = "AlgorithmSettingListView";
	private DragSortListView.DropListener onDrop = new DragSortListView.DropListener() {
		@Override
		public void drop(int from, int to) {
			if (from != to) {
				DragSortListView list = getListView();
				Log.d(TAG, "from " + from);

				AlgorithmItem from_item = m_adapter.getItem(from);
				m_adapter.remove(from_item);
				m_adapter.insert(from_item, to);
				list.moveCheckState(from, to);
			}
		}
	};

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		getWindow().setFlags(WindowManager.LayoutParams.FLAG_FULLSCREEN, WindowManager.LayoutParams.FLAG_FULLSCREEN);
		setContentView(R.layout.checkable_main);

		getActionBar().setDisplayShowTitleEnabled(false);
		getActionBar().setBackgroundDrawable(new ColorDrawable(0xFF0a84ad));
		getActionBar().setDisplayShowTitleEnabled(false);
		getActionBar().setHomeButtonEnabled(true);

		Intent intent = getIntent();
		algorithmData = (AlgorithmData) intent.getSerializableExtra("algorithmData");
		algorithmEnableData = (AlgorithmEnableData) intent.getSerializableExtra("algorithmEnableData");

		ArrayList<AlgorithmItem> mAlgoList = new ArrayList<AlgorithmItem>();
		String[] array = getResources().getStringArray(R.array.algorithmlist);
		algorithmCnt = array.length;
		Log.d(TAG, "" + array);

		int itemCnt = 0;
		for (String str : array) {
			AlgorithmItem p = new AlgorithmItem(str, (boolean) algorithmEnableData.algorithmEableList.get(itemCnt).get("algo"),
					itemCnt++);
			mAlgoList.add(p);
		}

		m_adapter = new AlgrithmAdapter(this, R.layout.setting_algorithm, mAlgoList);

		setListAdapter(m_adapter);
		list = getListView();
		list.setDropListener(onDrop);

	}

	public AlgorithmData getAlgorithmData() {

		return algorithmData;
	}

	public AlgorithmEnableData getAlgorithmEnableData() {

		return algorithmEnableData;
	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		MenuInflater inflater = getMenuInflater();
		inflater.inflate(R.menu.setting_menu_algo, menu);
		return super.onCreateOptionsMenu(menu);
	}

	@Override
	public boolean onOptionsItemSelected(MenuItem item) {
		// The action bar home/up action should open or close the drawer.
		// ActionBarDrawerToggle will take care of this.

		// Handle action buttons
		switch (item.getItemId()) {

		case android.R.id.home:
			Intent intent = new Intent();
			intent.putExtra("algorithmData", algorithmData);
			intent.putExtra("algorithmEnableData", algorithmEnableData);
			setResult(Activity.RESULT_OK, intent);
			finish();

			break;
		case R.id.action_save_algo:

			AlgorithmItem _p;
			for (int itemCnt = 0; itemCnt < algorithmCnt; itemCnt++) {
				_p = m_adapter.getItem(itemCnt);
				if (_p.isEnable()) {
					algorithmEnableData.algorithmEableList.get(itemCnt).put("algo", true);
				} else {
					algorithmEnableData.algorithmEableList.get(itemCnt).put("algo", false);
				}
			}
			break;
		}

		return super.onOptionsItemSelected(item);
	}

	@Override
	public DragSortListView getListView() {
		return (DragSortListView) super.getListView();
	}

	// Back key ó��
	@Override
	public boolean onKeyDown(int keyCode, KeyEvent event) {
		// TODO Auto-generated method stub

		switch (keyCode) {
		case KeyEvent.KEYCODE_BACK:
			Intent intent = new Intent();
			intent.putExtra("algorithmData", algorithmData);
			intent.putExtra("algorithmEnableData", algorithmEnableData);
			setResult(Activity.RESULT_OK, intent);
			finish();
			break;
		}
		return true;
	}

	@Override
	protected void onActivityResult(int requestCode, int resultCode, Intent data) {
		// TODO Auto-generated method stub
		super.onActivityResult(requestCode, resultCode, data);

		if (requestCode == REQ_CODE_PARA_SETTING) {
			if (resultCode == Activity.RESULT_OK) {
				Log.d(TAG, "catch from parametersettingListView");
				algorithmData = (AlgorithmData) data.getSerializableExtra("algorithmData");
				algorithmEnableData = (AlgorithmEnableData) data.getSerializableExtra("algorithmEnableData");

			}
		}
	}

}
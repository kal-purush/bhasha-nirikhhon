package com.example.pad;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import android.app.Activity;
import android.content.Context;
import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.AdapterView;
import android.widget.AdapterView.OnItemClickListener;
import android.widget.ArrayAdapter;
import android.widget.ListView;
import android.widget.Toast;

public class Editer extends Activity{

	static final String[] SONS = new String[] {

		"Afghanistan", "Albania", "Algeria", "American Samoa",
		"Andorra", "Angola", "Anguilla", "Antarctica",
		"Antigua and Barbuda", "Argentina", "Armenia", "Aruba",
		"Australia", "Austria", "Azerbaijan", "Bahrain",
		"Bangladesh", "Barbados", "Belarus", "Belgium", "Belize",
		"Benin", "Bermuda", "Bhutan", "Bolivia",
		"Bosnia and Herzegovina", "Botswana", "Bouvet Island",
		"Brazil", "British Indian Ocean Territory"
	};


	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		this.setContentView(R.layout.editer_activity);

		ListView listview = (ListView) findViewById(R.id.list);
		listview.setClickable(true);

		final ArrayList<String> list = new ArrayList<String>();
		for (int i = 0; i < SONS.length; ++i) {
			list.add(SONS[i]);
		}

		final StableArrayAdapter adapter = new StableArrayAdapter(this,
				android.R.layout.simple_list_item_1, list);
		listview.setAdapter(adapter);


		listview.setOnItemClickListener(new OnItemClickListener(){


			@Override
			public void onItemClick(AdapterView<?> parent, View view,
					int position, long id) {
			String item = (String) parent.getItemAtPosition(position);
			//Toast.makeText(getApplicationContext(), item, Toast.LENGTH_SHORT).show();
		    Intent i2 = new Intent();
	          i2.putExtra(MainActivity.EXTRA_MESSAGE, item);
	          Editer.this.setResult(1, i2);
	          Editer.this.finish();
			
			
			}


		});




		/*
	      final Intent intent = getIntent();
	      String message = intent.getStringExtra(MainActivity.EXTRA_MESSAGE);
	      final TextView tv1 = (TextView)findViewById( R.id.resultat );
	      tv1.setText( message );

	      Button b_retour =(Button)findViewById(R.id.bouton_retour);
	      b_retour.setOnClickListener( new OnClickListener() {
	        @Override
	        public void onClick(View v) {
	          Intent i2 = new Intent();
	          i2.putExtra(MainActivity.EXTRA_MESSAGE, "valeur de retour");
	          Activity2.this.setResult(1, i2);
	          Activity2.this.finish();
	        }
	      });
	    }*/



	}


	private class StableArrayAdapter extends ArrayAdapter<String> {

		HashMap<String, Integer> mIdMap = new HashMap<String, Integer>();

		public StableArrayAdapter(Context context, int textViewResourceId,
				List<String> objects) {
			super(context, textViewResourceId, objects);
			for (int i = 0; i < objects.size(); ++i) {
				mIdMap.put(objects.get(i), i);
			}
		}

		@Override
		public long getItemId(int position) {
			String item = getItem(position);
			return mIdMap.get(item);
		}

		@Override
		public boolean hasStableIds() {
			return true;
		}

	}



}
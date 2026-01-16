package com.xiaodevil.utils;

import java.util.ArrayList;
import java.util.List;

import android.content.Context;
import android.text.Editable;
import android.text.TextWatcher;
import android.view.LayoutInflater;
import android.view.MotionEvent;
import android.view.View;
import android.view.View.OnClickListener;
import android.view.View.OnTouchListener;
import android.view.ViewGroup;
import android.widget.AdapterView;
import android.widget.AdapterView.OnItemSelectedListener;
import android.widget.BaseAdapter;
import android.widget.EditText;
import android.widget.ImageButton;
import android.widget.LinearLayout;
import android.widget.ListView;
import android.widget.Spinner;

import com.xiaodevil.contacts.R;
import com.xiaodevil.models.PhoneNumber;
import com.xiaodevil.views.AddNewContactsActivity;

public class PhoneNumberAdapter extends BaseAdapter{
	private int resourceId;
	private Context context;
	private List<PhoneNumber> mObjects;
	private LinearLayout layout;
	
	private Integer index = -1;
	private class ButtonViewHolder{
		EditText PhoneNumber;
		Spinner type;
		ImageButton addNext;
	}
	private ButtonViewHolder holder;
	

	public PhoneNumberAdapter(Context context, int textViewResourceId, List<PhoneNumber> objects) {
		this.resourceId = textViewResourceId;
		this.context = context;
		this.mObjects = objects;
	
	}
	
	@Override
	public View getView(int position, View convertView, ViewGroup parent){
		if (convertView == null){
			layout = (LinearLayout)LayoutInflater.from(context).inflate(resourceId, null);
			holder = new ButtonViewHolder();
			
			holder.PhoneNumber = (EditText)layout.findViewById(R.id.input_phone_number);
			holder.type = (Spinner)layout.findViewById(R.id.spinner_number_type);
			holder.addNext = (ImageButton)layout.findViewById(R.id.new_phone_number);
			holder.addNext.setOnClickListener(new msgButtonListener(position, parent.getContext()));
			
			
			
			holder.PhoneNumber.setTag(position);
			holder.PhoneNumber.setOnTouchListener(new OnTouchListener() {
				
				@Override
				public boolean onTouch(View v, MotionEvent event) {
					if(event.getAction() == MotionEvent.ACTION_UP){
						index = (Integer)v.getTag();
					}
					return false;
				}
			});
			holder.PhoneNumber.addTextChangedListener(new MyTextWatcher(holder));
			holder.type.setOnItemSelectedListener(new mySpinnerListener(holder));
			layout.setTag(holder);
		}else{
			layout = (LinearLayout) convertView;
			holder = (ButtonViewHolder)layout.getTag();
			holder.PhoneNumber.setTag(position);		
		}
		
		PhoneNumber number = mObjects.get(position);
		if(number != null && !"".equals(number.getPhoneNumber())){
			holder.PhoneNumber.setText(number.getPhoneNumber());
		}else{
			String pnumber = mObjects.get(position).getPhoneNumber();
			holder.PhoneNumber.setText(pnumber);
		}
		holder.PhoneNumber.clearFocus();
		if(index != -1 && index == position){
			holder.PhoneNumber.requestFocus();
		}
		
		
		return layout;
		
	}
	class MyTextWatcher implements TextWatcher{
		private ButtonViewHolder mholder;
		
		public MyTextWatcher(ButtonViewHolder holder){
			this.mholder = holder;
		}
		@Override
		public void beforeTextChanged(CharSequence s, int start, int count,
				int after) {
			
		}

		@Override
		public void onTextChanged(CharSequence s, int start, int before,
				int count) {
						
		}

		@Override
		public void afterTextChanged(Editable s) {
			if(s != null && !"".equals(s.toString())){
				int position = (Integer)mholder.PhoneNumber.getTag();
				mObjects.get(position).setPhoneNumber(s.toString());
			}
			
		}
		
	}

	class msgButtonListener implements OnClickListener{
		private Context context;
		public msgButtonListener(int pos, Context con) {
			context = con;
		}
		@Override
		public void onClick(View v) {
			int vid = v.getId();
			if(vid == holder.addNext.getId()){
				mObjects.add(new PhoneNumber());
				AddNewContactsActivity.resetListViewHeight();
			}
		}
		
	}
	
	class mySpinnerListener implements OnItemSelectedListener{
		private ButtonViewHolder mholder;
		public mySpinnerListener(ButtonViewHolder holder){
			this.mholder = holder;
		}
		@Override
		public void onItemSelected(AdapterView<?> parent, View view,
				int position, long id) {
			int pos = (Integer)mholder.PhoneNumber.getTag();
			mObjects.get(pos).setType(position+1);
			
		}

		@Override
		public void onNothingSelected(AdapterView<?> parent) {
	
		}
		
	}
	
	@Override
	public int getCount() {
		return mObjects.size();
	}

	@Override
	public PhoneNumber getItem(int position) {
		return mObjects.get(position);	
	}
	
	public List<PhoneNumber> getAllItem(){
		return mObjects;
	}
	@Override
	public long getItemId(int position) {
		return position;
	}
	

	
}
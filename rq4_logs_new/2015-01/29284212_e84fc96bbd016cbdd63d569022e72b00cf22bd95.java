package com.xiaodevil.utils;

import java.util.ArrayList;
import java.util.List;

import android.content.Context;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ArrayAdapter;

public class UserInfoAdapter extends ArrayAdapter<String[]>{
	private int resourceId;

	public UserInfoAdapter(Context context, int textViewResourceId, List<String[]> objects) {
		super(context, textViewResourceId, objects);
		this.resourceId = textViewResourceId;
	}
	
	@Override
	public View getView(int position, View convertView, ViewGroup parent){
		
		
		
		return parent;
		
	}

}
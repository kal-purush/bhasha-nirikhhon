package com.castleburg;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.view.ViewGroup.LayoutParams;
import android.widget.BaseAdapter;
import android.widget.ImageView;
import android.widget.LinearLayout;

public class ResAdapter extends BaseAdapter {
	
	Context ctx;
	String[] res;
	LayoutInflater inflater;
	
	public ResAdapter(String[] mres,Context mctx){
		ctx=mctx;
		inflater = (LayoutInflater) ctx.getSystemService(Context.LAYOUT_INFLATER_SERVICE);
		res=mres;
	}

	@Override
	public int getCount() {
		return res.length;
	}

	@Override
	public Object getItem(int position) {
		return res[position];
	}

	@Override
	public long getItemId(int position) {
		return position;
	}

	@Override
	public View getView(int position, View convertView, ViewGroup parent) {
		if (convertView == null) convertView = (LinearLayout) inflater.inflate(R.layout.tess, parent, false);
		LinearLayout lin=(LinearLayout) convertView.findViewById(R.id.lin);
		lin.removeAllViews();
			for (int j=0;j<res[position].length();j++){
				lin.addView(getImage(res[position].charAt(j)));
			}
		return convertView;
	}
	
	
	private ImageView getImage(char c){
		LayoutParams linLayoutParam = new LayoutParams(LayoutParams.WRAP_CONTENT,  LayoutParams.WRAP_CONTENT);
		ImageView image=new ImageView(ctx);
		image.setLayoutParams(linLayoutParam);
		switch (c){
		case 'w':image.setImageResource(R.drawable.wood);
		break;
		case 'e':image.setImageResource(R.drawable.wood);
		break;
		case 's':image.setImageResource(R.drawable.stone);
		break;
		case 'd':image.setImageResource(R.drawable.stone);
		break;
		case 'g':image.setImageResource(R.drawable.gold);
		break;
		case 'h':image.setImageResource(R.drawable.gold);
		break;
		case 'p':image.setImageResource(R.drawable.winpoint);
		break;
		case '[':image.setImageResource(R.drawable.winpoint);
		break;
		case 'q':image.setImageResource(R.drawable.warpoint);
		break;
		case 'z':image.setImageResource(R.drawable.warpoint);
		break;
		case 'a':image.setImageResource(R.drawable.arrow);
		}
		return image;
	}
}
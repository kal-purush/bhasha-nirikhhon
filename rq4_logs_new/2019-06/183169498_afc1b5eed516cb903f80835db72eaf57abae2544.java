package com.community.shetuanbao.activity;
import android.app.Activity;
import android.content.Intent;
import android.graphics.Bitmap;
import android.os.Bundle;
import android.view.View;
import android.view.Window;
import android.widget.ImageView;
import android.widget.TextView;
import com.community.shetuanbao.R;
public class CheckAlbumActivity extends Activity{
	private String xiangcename=null;
	Bitmap bt=null;
	ImageView im;
	TextView fanhui;
	TextView shanchu;
	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		requestWindowFeature(Window.FEATURE_NO_TITLE);
		setContentView(R.layout.chakanxiangce);
		Intent intent = getIntent();
		xiangcename = intent.getStringExtra("name");
		bt = intent.getParcelableExtra("xiangce");
		im=(ImageView)findViewById(R.id.xiangcexiugai);
		fanhui=(TextView)findViewById(R.id.xiangce_juti_fanhui_2);
		fanhui.setOnClickListener(new View.OnClickListener() {
			@Override
			public void onClick(View v) {
				// TODO Auto-generated method stub
				finish();
			}
		});
		im.setImageBitmap(bt);
	}
}
package com.azz.mygifview;


import pl.droidsonroids.gif.GifDrawable;
import pl.droidsonroids.gif.GifImageView;

import android.os.Bundle;
import android.app.Activity;
import android.util.Log;

public class MainActivity extends Activity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        Log.d("azz", "onCreate");
        GifImageView gib = (GifImageView) findViewById(R.id.hisGifView);
        
        /**
         * 调用项目资源
         */
		try {
			GifDrawable gifFromResource = new GifDrawable( getResources(), R.drawable.run );
			gib.setImageDrawable(gifFromResource);
		} catch (Exception e) {
			e.printStackTrace();
		}
		/**
		 * 调用本地文件
		 */
//		try {
//			GifDrawable gifFromResource = new GifDrawable("/mnt/sda/sda1/picture/anim.gif");
//			gib.setImageDrawable(gifFromResource);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}

        

        Log.d("azz", "onCreate-end");
    }

}
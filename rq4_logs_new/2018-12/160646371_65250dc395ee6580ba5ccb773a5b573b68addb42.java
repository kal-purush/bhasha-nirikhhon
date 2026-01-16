package com.boredream.videoplayer;

import android.app.Activity;
import android.content.ContentResolver;
import android.content.Intent;
import android.database.Cursor;
import android.graphics.Bitmap;
import android.net.Uri;
import android.os.Bundle;
import android.provider.MediaStore;
import android.support.v7.app.AppCompatActivity;
import android.view.View;
import android.widget.Toast;

import java.io.File;

public class MainActivity extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        setContentView(R.layout.activity_main);

        findViewById(R.id.btn_start).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                VideoDetailInfo info = MockUtils.mockData(VideoDetailInfo.class, null);
                VideoDetailActivity.start(MainActivity.this, info);
            }
        });

        findViewById(R.id.btn_open).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent intent = new Intent(Intent.ACTION_GET_CONTENT);
                intent.setType("video/*");//设置类型
                intent.addCategory(Intent.CATEGORY_OPENABLE);//调用文件管理器
                startActivityForResult(intent,1);
            }
        });
    }


    @Override
    protected void onActivityResult(int requestCode, int resultCode, Intent data) {
        if (resultCode == Activity.RESULT_OK) {//是否选择，没选择就不会继续
            Uri uri = data.getData();//得到uri
            String[] proj = {MediaStore.Images.Media.DATA};
            Cursor actualimagecursor = getContentResolver().query(uri, proj, null, null, null);
            int actual_image_column_index = actualimagecursor.getColumnIndexOrThrow(MediaStore.Images.Media.DATA);
            actualimagecursor.moveToFirst();
            String img_path = actualimagecursor.getString(actual_image_column_index);
            //img_path = getPath(this, uri).toString() || uri.getPath().toString()
            actualimagecursor.close();
            VideoDetailInfo info = MockUtils.mockData(VideoDetailInfo.class, img_path);
            VideoDetailActivity.start(MainActivity.this, info);
        }
    }
}
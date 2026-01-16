package com.andygu.sample_02.activity;

import android.Manifest;
import android.content.pm.PackageManager;
import android.support.annotation.NonNull;
import android.support.v4.app.ActivityCompat;
import android.support.v7.app.AlertDialog;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import com.andygu.sample_02.R;

import static android.Manifest.permission.READ_EXTERNAL_STORAGE;

public class PictureActivity extends AppCompatActivity {

  private static final int REQUEST_READ_STORAGE = 3;

  @Override protected void onCreate(Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);
    setContentView(R.layout.activity_picture);
    int permission = ActivityCompat.checkSelfPermission(this, READ_EXTERNAL_STORAGE);
    if(permission != PackageManager.PERMISSION_GRANTED){
      //未取得權限,向使用者要求允許權限
      ActivityCompat.requestPermissions(this,new String[]{READ_EXTERNAL_STORAGE},REQUEST_READ_STORAGE);
    }else{
      //已有權限,可進行檔案存取
      readThumbnail();
    }
  }

  @Override
  public void onRequestPermissionsResult(int requestCode, @NonNull String[] permissions, @NonNull int[] grantResults) {
    super.onRequestPermissionsResult(requestCode, permissions, grantResults);
    switch (requestCode){
      case REQUEST_READ_STORAGE:{
        if(grantResults.length > 0 && grantResults[0] == PackageManager.PERMISSION_GRANTED){
          //取得讀取外部儲存權限,進行存取
          readThumbnail();
        } else {
          //使用者拒絕權限,顯示對話框提示
          new AlertDialog.Builder(this).setMessage("必須允許讀取外部儲存權限才能顯示圖檔").setPositiveButton("OK",null).show();
        }
        break;
      }default:{
        break;
      }
    }
  }

  private void readThumbnail() {
  }
}
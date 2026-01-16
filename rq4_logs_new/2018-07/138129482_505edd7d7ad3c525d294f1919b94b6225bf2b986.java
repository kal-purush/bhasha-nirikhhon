package com.andygu.sample_02.activity;

import android.Manifest;
import android.content.ContentResolver;
import android.content.pm.PackageManager;
import android.database.Cursor;
import android.provider.ContactsContract;
import android.support.annotation.NonNull;
import android.support.v4.app.ActivityCompat;
import android.support.v7.app.AlertDialog;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.util.Log;
import android.widget.ListView;
import android.widget.SimpleCursorAdapter;
import com.andygu.sample_02.R;

import static android.Manifest.permission.READ_CONTACTS;
import static android.Manifest.permission.WRITE_CONTACTS;

public class ContactsContentActivity extends AppCompatActivity {

  private ListView lvContacts;

  private static final int REQUEST_CONTACTS = 1;

  @Override protected void onCreate(Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);
    setContentView(R.layout.activity_contacts_content);
    findViews();
    int premission = ActivityCompat.checkSelfPermission(this, READ_CONTACTS);
    if(premission != PackageManager.PERMISSION_GRANTED){
      //未取得權限,向使用者要求允許權限
      ActivityCompat.requestPermissions(this,new String[]{READ_CONTACTS,WRITE_CONTACTS},REQUEST_CONTACTS);
    } else {
      readContacts();
    }

  }

  public void findViews(){
    lvContacts = findViewById(R.id.lv_contacts);
  }

  @Override
  public void onRequestPermissionsResult(int requestCode, @NonNull String[] permissions, @NonNull int[] grantResults) {
    super.onRequestPermissionsResult(requestCode, permissions, grantResults);
    switch(requestCode){
      case REQUEST_CONTACTS:{
        if(grantResults.length > 0 && grantResults[0] == PackageManager.PERMISSION_GRANTED){
          //取得聯絡人權限,進行存取
          readContacts();
        } else {
          new AlertDialog.Builder(this).setMessage("必須允許聯絡人權限,才能顯示資料").setPositiveButton("OK",null).show();
        }
        break;
      }
      default:{
        break;
      }
    }
  }

  private void readContacts() {
    ContentResolver resolver = getContentResolver();
    Cursor cursor = resolver.query(ContactsContract.Contacts.CONTENT_URI,null,null,null,null);
    //開發除錯用
    //while (cursor.moveToNext()){
    //  int id = cursor.getInt(cursor.getColumnIndex(ContactsContract.Contacts._ID));
    //  String name = cursor.getString(cursor.getColumnIndex(ContactsContract.Contacts.DISPLAY_NAME));
    //  Log.d("RECORD",id + " / " + name);
    //}

    //填入Adapter
    SimpleCursorAdapter simpleCursorAdapter = new SimpleCursorAdapter(this,android.R.layout.simple_list_item_1,cursor
        ,new String[]{ContactsContract.Contacts.DISPLAY_NAME},new int[]{android.R.id.text1},1);
    lvContacts.setAdapter(simpleCursorAdapter);
  }

}
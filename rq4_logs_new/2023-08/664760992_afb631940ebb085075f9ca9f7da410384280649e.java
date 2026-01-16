package com.example.travelappfragment;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.annotation.RequiresApi;
import androidx.appcompat.app.AppCompatActivity;
import androidx.core.content.ContextCompat;

import android.Manifest;
import android.content.ContentValues;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.database.sqlite.SQLiteDatabase;
import android.graphics.Bitmap;
import android.graphics.drawable.BitmapDrawable;
import android.net.Uri;
import android.os.Build;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ImageView;
import android.widget.Toast;

import com.squareup.picasso.Picasso;
import com.theartofdev.edmodo.cropper.CropImage;

import java.io.ByteArrayOutputStream;

public class EditProfile extends AppCompatActivity {

    DatabaseHelper databaseHelper;
    SQLiteDatabase sqLiteDatabase;
    ImageView avatar;
    EditText name,password, nickname, phone;
    Button submit;
    int id=0;

    public static final int CAMERA_REQUEST=100;
    public static final int STORAGE_REQUEST=101;
    String[]cameraPermission;
    String[]storagePermission;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_edit_profile);
        cameraPermission=new String[]{android.Manifest.permission.CAMERA, android.Manifest.permission.WRITE_EXTERNAL_STORAGE};
        storagePermission=new String[]{Manifest.permission.WRITE_EXTERNAL_STORAGE};

        databaseHelper = new DatabaseHelper(this);
        findid();
        insertData();
        imagePick();
    }

    private void imagePick() {
        avatar.setOnClickListener(new View.OnClickListener() {
            @RequiresApi(api = Build.VERSION_CODES.M)
            @Override
            public void onClick(View view) {
                int avatar=0;
                if (avatar==0){
                    if (!checkCameraPermission()){
                        requestCameraPermission();
                    }else {
                        pickFromGallery();
                    }
                }else if (avatar==1){
                    if (!checkStoragePermission()){
                        requestStoragePermission();
                    }else{
                        pickFromGallery();
                    }
                }
            }
        });
    }
    @RequiresApi(api = Build.VERSION_CODES.M)
    private void requestStoragePermission() {
        requestPermissions(storagePermission,STORAGE_REQUEST);
    }

    private boolean checkStoragePermission() {
        boolean result= ContextCompat.checkSelfPermission(this,Manifest.permission.WRITE_EXTERNAL_STORAGE)==(PackageManager.PERMISSION_GRANTED);
        return result;
    }

    private void pickFromGallery() {
        CropImage.activity().start(this);
    }

    @RequiresApi(api = Build.VERSION_CODES.M)
    private void requestCameraPermission() {
        requestPermissions(cameraPermission,CAMERA_REQUEST);
    }

    private boolean checkCameraPermission() {
        boolean result= ContextCompat.checkSelfPermission(this, Manifest.permission.WRITE_EXTERNAL_STORAGE)==(PackageManager.PERMISSION_GRANTED);
        boolean result2=ContextCompat.checkSelfPermission(this, Manifest.permission.CAMERA)==(PackageManager.PERMISSION_GRANTED);
        return result && result2;
    }

    private void insertData() {
        submit.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                ContentValues cv=new ContentValues();
                cv.put("avatar",ImageViewToByte(avatar));
                cv.put("name",name.getText().toString());
                cv.put("password",name.getText().toString());
                cv.put("nickname",name.getText().toString());
                cv.put("phone",name.getText().toString());
                sqLiteDatabase=databaseHelper.getWritableDatabase();
                Long recinsert=sqLiteDatabase.insert("profile",null,cv);
                if (recinsert!=null){
                    Toast.makeText(EditProfile.this, "inserted successfully", Toast.LENGTH_SHORT).show();
                    //clear when click on submit button
                    avatar.setImageResource(R.mipmap.ic_launcher);
                    name.setText("");
                }
            }
        });
    }

    private byte[] ImageViewToByte(ImageView avatar) {
        Bitmap bitmap=((BitmapDrawable)avatar.getDrawable()).getBitmap();
        ByteArrayOutputStream stream=new ByteArrayOutputStream();
        bitmap.compress(Bitmap.CompressFormat.JPEG,80,stream);
        byte[]bytes=stream.toByteArray();
        return bytes;
    }

    private void findid() {
        avatar=(ImageView) findViewById(R.id.avatar);
        name=(EditText)findViewById(R.id.name);
        password=(EditText)findViewById(R.id.password);
        nickname=(EditText)findViewById(R.id.nick_name);
        phone=(EditText)findViewById(R.id.phone);
        submit=(Button)findViewById(R.id.btn_submit);
    }

    @Override
    public void onRequestPermissionsResult(int requestCode, @NonNull String[] permissions, @NonNull int[] grantResults) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults);
        switch (requestCode){
            case CAMERA_REQUEST:{
                if (grantResults.length>0){
                    boolean camera_accept=grantResults[0]==PackageManager.PERMISSION_GRANTED;
                    boolean storage_accept=grantResults[1]==PackageManager.PERMISSION_GRANTED;
                    if (camera_accept&&storage_accept){
                        pickFromGallery();
                    }else{
                        Toast.makeText(this, "enable camera and storage permission", Toast.LENGTH_SHORT).show();
                    }
                }
            }
            break;
            case STORAGE_REQUEST:{
                if (grantResults.length>0){
                    boolean storage_accept=grantResults[0]==PackageManager.PERMISSION_GRANTED;
                    if (storage_accept){
                        pickFromGallery();
                    }else{
                        Toast.makeText(this, "please enable storage permission", Toast.LENGTH_SHORT).show();
                    }
                }
            }
            break;
        }
    }

    @Override
    protected void onActivityResult(int requestCode, int resultCode, @Nullable Intent data) {
        super.onActivityResult(requestCode, resultCode, data);
        if (requestCode==CropImage.CROP_IMAGE_ACTIVITY_REQUEST_CODE){
            CropImage.ActivityResult result=CropImage.getActivityResult(data);
            if (resultCode==RESULT_OK){
                Uri resultUri=result.getUri();
                Picasso.with(this).load(resultUri).into(avatar);
            }
        }
    }
}
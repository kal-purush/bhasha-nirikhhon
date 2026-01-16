package com.example.caucse.baseui;

import android.Manifest;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.database.Cursor;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.graphics.Matrix;
import android.media.ExifInterface;
import android.net.Uri;
import android.os.Bundle;
import android.provider.MediaStore;
import android.support.v4.app.ActivityCompat;
import android.support.v4.content.ContextCompat;
import android.support.v7.app.AppCompatActivity;
import android.widget.ImageView;
import android.widget.TextView;
import android.widget.Toast;

import java.io.File;
import java.io.IOException;

public class Album_Activity extends AppCompatActivity{
    private static final int PICK_FROM_CAMERA = 0;
    private Uri mImageCaptureUri;

    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        doTakeAlbumAction();


        /// 맥주판별 알고리즘 들어가는 곳
        TextView beerName = (TextView) findViewById(R.id.name);
        TextView beerCountry = (TextView) findViewById(R.id.country);
        TextView beerFlavor = (TextView) findViewById(R.id.flavor);
        TextView beerKind = (TextView) findViewById(R.id.kind);
        TextView beerIBU = (TextView) findViewById(R.id.ibu);
        TextView beerAlcohol = (TextView) findViewById(R.id.alcohol);
        TextView beerKcal = (TextView) findViewById(R.id.kcal);
        DBHandler dbHandler = DBHandler.open(this);
        try {
            int ID = 3; // 3번째인 cass로 가정한다.
            Cursor cursor = dbHandler.select(ID);
            if (cursor.getCount() == 0) {
                Toast.makeText(this, "데이터가 없습니다.",
                        Toast.LENGTH_SHORT).show();
            } else {
                String Name = cursor.getString(cursor.getColumnIndex("Name"));
                String Country = cursor.getString(cursor.getColumnIndex("Country"));
                String Flavor = cursor.getString(cursor.getColumnIndex("Flavor"));
                String Kind = cursor.getString(cursor.getColumnIndex("Kind"));
                Float IBU = cursor.getFloat(cursor.getColumnIndex("IBU"));
                Float Alcohol = cursor.getFloat(cursor.getColumnIndex("Alcohol"));
                int kcal = cursor.getInt(cursor.getColumnIndex("kcal"));
                beerName.setText(Name);
                beerCountry.setText(Country);
                beerFlavor.setText(Flavor);
                beerKind.setText(Kind);
                beerIBU.setText(String.valueOf(IBU));
                beerAlcohol.setText(String.valueOf(Alcohol));
                beerKcal.setText(String.valueOf(kcal));
            }
            cursor.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onActivityResult(int requestCode, int resultCode, Intent data){
        if (requestCode == PICK_FROM_CAMERA && resultCode == RESULT_OK) {
            setContentView(R.layout.activity_album);
            mImageCaptureUri = data.getData();
            String[] projection = { MediaStore.Images.Media.DATA};

            grantUriPermission();
            Cursor mCursor = getContentResolver().query(mImageCaptureUri, projection, null, null, null);
            mCursor.moveToFirst();
            int column_index = mCursor.getColumnIndexOrThrow(MediaStore.Images.Media.DATA);

            String path = mCursor.getString(column_index);
            if(mCursor!=null){
                mCursor.close();
                mCursor = null;
            }
            Bitmap bitmap = BitmapFactory.decodeFile(path);
            ExifInterface exif = null;

            try {
                exif = new ExifInterface(path);
            } catch (IOException e) {
                e.printStackTrace();
            }

            int exifOrientation;
            int exifDegree;

            if (exif != null) {
                exifOrientation = exif.getAttributeInt(ExifInterface.TAG_ORIENTATION, ExifInterface.ORIENTATION_NORMAL);
                exifDegree = exifOrientationToDegrees(exifOrientation);
            } else {
                exifDegree = 0;
            }
            ((ImageView)findViewById(R.id.img)).setImageBitmap(rotate(bitmap, exifDegree));
        }else{
            finish();
        }
    }

    //상수를 받아 각도로 변환시켜주는 메소드
    private int exifOrientationToDegrees(int exifOrientation) {
        if (exifOrientation == ExifInterface.ORIENTATION_ROTATE_90) {
            return 90;
        } else if (exifOrientation == ExifInterface.ORIENTATION_ROTATE_180) {
            return 180;
        } else if (exifOrientation == ExifInterface.ORIENTATION_ROTATE_270) {
            return 270;
        }
        return 0;
    }
    //비트맵을 각도대로 회전시켜 결과를 반환해주는 메소드이다.
    private Bitmap rotate(Bitmap bitmap, float degree) {
        Matrix matrix = new Matrix();
        matrix.postRotate(degree);
        return Bitmap.createBitmap(bitmap, 0, 0, bitmap.getWidth(), bitmap.getHeight(), matrix, true);
    }

    public void doTakeAlbumAction(){
        
        grantUriPermission();
        Intent intent = new Intent(Intent.ACTION_PICK);
        intent.setType(MediaStore.Images.Media.CONTENT_TYPE);
        startActivityForResult(intent, PICK_FROM_CAMERA);
        
        
    }

    private void grantUriPermission() {
        //갤러리, 카메라 사용 권한 체크

        if(ContextCompat.checkSelfPermission(Album_Activity.this, Manifest.permission.WRITE_EXTERNAL_STORAGE) !=
                PackageManager.PERMISSION_GRANTED){
            //최초 권한 요청인지 혹은 사용자에게 의한 재요청인지 확인
            if(ActivityCompat.shouldShowRequestPermissionRationale(Album_Activity.this, Manifest.permission.WRITE_EXTERNAL_STORAGE)) {
                //사용자가 임의로 권한을 취소하면 권한 재요청
                ActivityCompat.requestPermissions(Album_Activity.this, new String[]{Manifest.permission.WRITE_EXTERNAL_STORAGE}, 1);
            } else {
                ActivityCompat.requestPermissions(Album_Activity.this, new String[]{Manifest.permission.WRITE_EXTERNAL_STORAGE}, 1);
            }
        } else if(ContextCompat.checkSelfPermission(Album_Activity.this, Manifest.permission.READ_EXTERNAL_STORAGE) !=
                PackageManager.PERMISSION_GRANTED){
            //최초 권한인지 혹은 사용자에게 의한 재요청인지 확인
            if(ActivityCompat.shouldShowRequestPermissionRationale(Album_Activity.this, Manifest.permission.READ_EXTERNAL_STORAGE)){
                // 사용자가 임의로 권한을 취소하면 권한재요청
                ActivityCompat.requestPermissions(Album_Activity.this, new String[]{Manifest.permission.READ_EXTERNAL_STORAGE}, 1);
            }else {
            ActivityCompat.requestPermissions(Album_Activity.this,  new String[]{Manifest.permission.READ_EXTERNAL_STORAGE}, 1 );
            }
        }else if(ContextCompat.checkSelfPermission(Album_Activity.this, Manifest.permission.CAMERA) != PackageManager.PERMISSION_GRANTED)
            // 최초 권한 요청인지 혹은 사용자에게 의한 재요청인지 확인
            if(ActivityCompat.shouldShowRequestPermissionRationale(Album_Activity.this, Manifest.permission.CAMERA)){
            // 사용자가 임의로 권한을 취소 시킨 경우, 권한 재요청
                ActivityCompat.requestPermissions(Album_Activity.this, new String[]{Manifest.permission.CAMERA}, 1);
            }else {
                ActivityCompat.requestPermissions(Album_Activity.this, new String[]{Manifest.permission.CAMERA}, 1);
            }
    }
}
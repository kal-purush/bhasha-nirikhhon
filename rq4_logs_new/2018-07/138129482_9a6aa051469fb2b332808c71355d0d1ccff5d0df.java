package com.andygu.sample_02.activity;

import android.content.CursorLoader;
import android.database.Cursor;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.provider.MediaStore;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.GestureDetector;
import android.view.MotionEvent;
import android.widget.ImageView;
import com.andygu.sample_02.R;

public class PictureDetailActivity extends AppCompatActivity implements GestureDetector.OnGestureListener{
  
  private int position;
  private ImageView image;
  private Cursor cursor;
  private GestureDetector gestureDetector;

  @Override protected void onCreate(Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);
    setContentView(R.layout.activity_picture_detail);
    position = getIntent().getIntExtra("POSITION",0);
    image = findViewById(R.id.imageView);
    CursorLoader loader = new CursorLoader(this
        , MediaStore.Images.Media.EXTERNAL_CONTENT_URI
        ,null,null,null,null);
    cursor = loader.loadInBackground();
    cursor.moveToPosition(position);
    updateImage();
    //手勢滑動功能
    gestureDetector = new GestureDetector(this,this);
  }

  private void updateImage() {
    String imagePath = cursor.getString(cursor.getColumnIndex(MediaStore.Images.Media.DATA));
    Bitmap bitmap = BitmapFactory.decodeFile(imagePath);
    image.setImageBitmap(bitmap);
  }

  //改為交由GestureDetector處理
  @Override public boolean onTouchEvent(MotionEvent event) {
    return gestureDetector.onTouchEvent(event);
  }

  //在畫面中按下時
  @Override public boolean onDown(MotionEvent e) {
    return false;
  }
  //輕碰螢幕當未放開時
  @Override public void onShowPress(MotionEvent e) {

  }
  //輕觸螢幕放開時
  @Override public boolean onSingleTapUp(MotionEvent e) {
    return false;
  }
  //當使用者按下後移動時
  @Override public boolean onScroll(MotionEvent e1, MotionEvent e2, float distanceX, float distanceY) {
    return false;
  }
  //常按螢幕兩秒含以上後
  @Override public void onLongPress(MotionEvent e) {

  }

  /**
   * 使用者快速滑動螢幕後放開時
   * @param e1                  滑動起始點
   * @param e2                  滑動結束點
   * @param velocityX       橫向移動的速度值
   * @param velocityY       直向移動的速度值
   * @return
   */
  @Override public boolean onFling(MotionEvent e1, MotionEvent e2, float velocityX, float velocityY) {
    //左滑為負值,右滑為正值
    float distance = e2.getX()-e1.getX();
    if(distance>100){
      //向右,往前一張圖無資料回最後一張
      if(!cursor.moveToPrevious()){
        cursor.moveToLast();
      }
      //刷新
      updateImage();
    } else if(distance<-100){
      //向左,往後一張圖,無資料回第一張
      if(!cursor.moveToNext()){
        cursor.moveToFirst();
      }
      //刷新
      updateImage();
    }
    return false;
  }
}
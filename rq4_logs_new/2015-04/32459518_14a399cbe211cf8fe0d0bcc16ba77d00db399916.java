package com.freud.mrzz.atys;

import android.app.Activity;
import android.content.DialogInterface;
import android.content.Intent;
import android.graphics.Bitmap;
import android.graphics.Point;
import android.net.Uri;
import android.os.Bundle;
import android.os.Environment;
import android.os.Handler;
import android.os.Message;
import android.provider.MediaStore;
import android.util.Log;
import android.view.Display;
import android.view.LayoutInflater;
import android.view.MotionEvent;
import android.view.View;
import android.view.ViewGroup;
import android.widget.FrameLayout;
import android.widget.ImageView;
import android.widget.LinearLayout;
import android.widget.PopupWindow;
import android.widget.TextView;

import com.freud.mrzz.R;
import com.freud.mrzz.net.NetCore;
import com.freud.mrzz.utils.T;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Created by rival on 2015/4/15.
 */
public class pubTopicActivity extends Activity implements View.OnClickListener {
    private LinearLayout ll_selectcommunity;
    private PopupWindow mPopupwindow;
    private TextView tv_shaimeizhuang, tv_shaifuzhuang, tv_guanshuiqu, tv_community;
    private LinearLayout ll_images;
    private ImageView iv_addimages, iv_getback, iv_finishedit;
    private Bitmap imageUpload;
    private static String path = Environment.getExternalStorageDirectory().getPath()+"mrzz";

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_pubtopic);
        initView();
    }

    private void initView() {
        ll_selectcommunity = (LinearLayout) findViewById(R.id.ll_selectcommunity_pubtopicaty);
        tv_community = (TextView) findViewById(R.id.tv_community_pubaty);
        iv_addimages = (ImageView) findViewById(R.id.iv_addimage_pubaty);
        iv_getback = (ImageView) findViewById(R.id.iv_getback_pubaty);
        iv_finishedit = (ImageView) findViewById(R.id.iv_finishedit_pubaty);
        ll_images = (LinearLayout) findViewById(R.id.ll_images_pubaty);
        ll_selectcommunity.setOnClickListener(this);
        iv_addimages.setOnClickListener(this);
        iv_getback.setOnClickListener(this);
        iv_finishedit.setOnClickListener(this);
    }

    private void initPopupwindow() {
        mPopupwindow = new PopupWindow();
        LayoutInflater inflater = LayoutInflater.from(this);
        View layout = inflater.inflate(R.layout.popupwindow_selcom_pubaty, null);
        mPopupwindow.setContentView(layout);
        tv_shaifuzhuang = (TextView) layout.findViewById(R.id.tv_shaifuzhuang_popup_pubaty);
        tv_shaimeizhuang = (TextView) layout.findViewById(R.id.tv_shaimeizhuang_popup_pubaty);
        tv_guanshuiqu = (TextView) layout.findViewById(R.id.tv_guanshuiqu_popup_pubaty);
        tv_guanshuiqu.setOnClickListener(this);
        tv_shaifuzhuang.setOnClickListener(this);
        tv_shaimeizhuang.setOnClickListener(this);
        Display display = getWindowManager().getDefaultDisplay();
        Point size = new Point();
        display.getSize(size);
        int windowWidth = size.x;
        mPopupwindow
                .setHeight(ViewGroup.LayoutParams.WRAP_CONTENT);
        mPopupwindow
                .setWidth(windowWidth * 9 / 10);


    }

    @Override
    public void onClick(View v) {
        switch (v.getId()) {
            case R.id.ll_selectcommunity_pubtopicaty:
                if (mPopupwindow == null) {
                    initPopupwindow();
                    mPopupwindow.showAsDropDown(v, 0, 20);
                    mPopupwindow.update();
                } else {
                    mPopupwindow.dismiss();
                    mPopupwindow = null;
                }
                break;
            case R.id.tv_shaifuzhuang_popup_pubaty:
                tv_community.setText(tv_shaifuzhuang.getText());
                mPopupwindow.dismiss();
                mPopupwindow = null;
                break;
            case R.id.tv_shaimeizhuang_popup_pubaty:
                tv_community.setText(tv_shaimeizhuang.getText());
                mPopupwindow.dismiss();
                mPopupwindow = null;
                break;
            case R.id.tv_guanshuiqu_popup_pubaty:
                tv_community.setText(tv_guanshuiqu.getText());
                mPopupwindow.dismiss();
                mPopupwindow = null;
                break;
            case R.id.iv_addimage_pubaty:
                new android.app.AlertDialog.Builder(this)
                        .setTitle("照片选择")
                        .setNegativeButton("相册选取",
                                new DialogInterface.OnClickListener() {

                                    @Override
                                    public void onClick(DialogInterface dialog,
                                                        int which) {
                                        dialog.cancel();
                                        Intent intent1 = new Intent(
                                                Intent.ACTION_PICK, null);
                                        intent1.setDataAndType(
                                                MediaStore.Images.Media.EXTERNAL_CONTENT_URI,
                                                "image/*");
                                        startActivityForResult(intent1, 1);
                                    }
                                })
                        .setPositiveButton("相机拍照",
                                new DialogInterface.OnClickListener() {

                                    @Override
                                    public void onClick(DialogInterface dialog,
                                                        int which) {
                                        dialog.cancel();
                                        String status = Environment
                                                .getExternalStorageState();
                                        if (status
                                                .equals(Environment.MEDIA_MOUNTED)) {// 判断是否有SD卡
                                            Intent intent2 = new Intent(
                                                    MediaStore.ACTION_IMAGE_CAPTURE);
                                            intent2.putExtra(
                                                    MediaStore.EXTRA_OUTPUT,
                                                    Uri.fromFile(new File(
                                                            Environment
                                                                    .getExternalStorageDirectory(),
                                                            "head.jpg")));
                                            startActivityForResult(intent2, 2);// 采用ForResult打开
                                        }
                                    }
                                }).show();
                break;
            case R.id.iv_finishedit_pubaty:
                 T.showShort(getApplicationContext(), "finish");
                NetCore netCore = new NetCore();
                NetThread netThread = new NetThread(netCore,NetCore.URL_HEAD+"forum/file_test.php",Environment.getExternalStorageDirectory().getPath()
                        + "/head.jpg");
                netThread.start();

                break;
            case R.id.iv_getback_pubaty:
                finish();
                break;
        }
    }

    protected void onActivityResult(int requestCode, int resultCode, Intent data) {
        switch (requestCode) {
            case 1:
                if (resultCode == RESULT_OK) {
                    cropPhoto(data.getData());// 裁剪图片
                }

                break;
            case 2:
                if (resultCode == RESULT_OK) {
                    File temp = new File(Environment.getExternalStorageDirectory()
                            + "/head.jpg");
                    cropPhoto(Uri.fromFile(temp));// 裁剪图片
                }

                break;
            case 3:
                if (data != null) {
                    Bundle extras = data.getExtras();
                    imageUpload = extras.getParcelable("data");
                    if (imageUpload != null) {
                        /**
                         * 上传服务器代码
                         */
                        setPicToView(imageUpload);// 保存在SD卡中
                        addNewImages();
                    }
                }
                break;
            default:
                break;

        }
        super.onActivityResult(requestCode, resultCode, data);
    };
    private void addNewImages(){
        final FrameLayout mframelayout = new FrameLayout(this);
        ImageView iv_image_new = new ImageView(this);
        ImageView iv_image_delete_new = new ImageView(this);
        mframelayout.addView(iv_image_new);
        mframelayout.addView(iv_image_delete_new);
        mframelayout.setPadding(20, 20, 20, 20);
        iv_image_new.setImageBitmap(imageUpload);
        iv_image_delete_new.setImageResource(R.drawable.delete_image_pubaty_blue);
        FrameLayout.LayoutParams layoutParams = new FrameLayout.LayoutParams(250,250);
        mframelayout.setLayoutParams(layoutParams);
        ViewGroup.LayoutParams layoutParams1 = iv_image_delete_new.getLayoutParams();
        layoutParams1.height=50;
        layoutParams1.width=50;
        iv_image_delete_new.setLayoutParams(layoutParams1);
        iv_image_delete_new.setOnClickListener(new View.OnClickListener(){
            @Override
            public void onClick(View v) {
                ll_images.removeView(mframelayout);
            }
        });
        ll_images.addView(mframelayout,0);

    }

    public void cropPhoto(Uri uri) {
        Intent intent = new Intent("com.android.camera.action.CROP");
        intent.setDataAndType(uri, "image/*");
        intent.putExtra("crop", "true");
        // aspectX aspectY 是宽高的比例
        intent.putExtra("aspectX", 1);
        intent.putExtra("aspectY", 1);
        // outputX outputY 是裁剪图片宽高
        intent.putExtra("outputX", 150);
        intent.putExtra("outputY", 150);
        intent.putExtra("return-data", true);
        startActivityForResult(intent, 3);
    }

    //保存头像
    private void setPicToView(Bitmap mBitmap) {
        String sdStatus = Environment.getExternalStorageState();
        if (!sdStatus.equals(Environment.MEDIA_MOUNTED)) { // 检测sd是否可用
            return;
        }
        FileOutputStream b = null;
        File file = new File(path);
        file.mkdirs();// 创建文件夹
        String fileName = path + "uploadcache.jpg";// 图片名字
        try {
            b = new FileOutputStream(fileName);
            mBitmap.compress(Bitmap.CompressFormat.JPEG, 100, b);// 把数据写入文件

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                // 关闭流
                b.flush();
                b.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    @Override
    public boolean onTouchEvent(MotionEvent event) {
        if (mPopupwindow != null && mPopupwindow.isShowing()) {
            mPopupwindow.dismiss();
            mPopupwindow = null;
        }
        return super.onTouchEvent(event);

    }
    Handler mHandler = new Handler(){
        @Override
        public void handleMessage(Message msg) {
            super.handleMessage(msg);
            switch (msg.what){
                case 0:
                    T.showShort(getApplicationContext(),msg.obj.toString());
                    Log.e("zyh",msg.obj.toString());
                    break;
            }

        }
    };
    class NetThread extends Thread{
        NetCore netCore;
        String path;
        String url;
        public NetThread(NetCore netCore,String url,String path){
            this.netCore = netCore;
            this.url =url;
            this.path = path;
        }

        @Override
        public void run() {
            super.run();
            try {
                Log.e("hahaha",path);
                Log.e("hahaha",netCore.UploadAvatatest(url,path));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

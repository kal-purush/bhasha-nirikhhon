package com.example.filedemo.download;

import android.content.ContentValues;
import android.net.Uri;
import android.os.Bundle;
import android.os.Environment;
import android.provider.MediaStore;
import android.view.View;

import androidx.appcompat.app.AppCompatActivity;

import com.example.filedemo.R;

import java.io.BufferedOutputStream;
import java.io.OutputStream;

/**
 * 操作download文件夹
 */
public class DownloadActivity extends AppCompatActivity {

    private static final String TAG = DownloadActivity.class.getSimpleName();
    

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_download);
    }

    /**
     * 保存文本文件到download
     * @param v
     */
    public void onSave(View v) {
        createTextFile();
    }

    /**
     * 创建文本文件
     * 在 Download 目录下创建 hello.txt
     */
    private void createTextFile(){

        // 操作 external.db 数据库
        // 获取 Uri 路径
        Uri uri = MediaStore.Files.getContentUri("external");

        // 将要新建的文件的文件索引插入到 external.db 数据库中
        // 需要插入到 external.db 数据库 files 表中, 这里就需要设置一些描述信息
        ContentValues contentValues = new ContentValues();

        // 设置插入 external.db 数据库中的 files 数据表的各个字段的值

        // 设置存储路径 , files 数据表中的对应 relative_path 字段在 MediaStore 中以常量形式定义
        contentValues.put(MediaStore.Downloads.RELATIVE_PATH, Environment.DIRECTORY_DOWNLOADS + "/hello");
        // 设置文件名称
        contentValues.put(MediaStore.Downloads.DISPLAY_NAME, "hello.txt");
        // 设置文件标题, 一般是删除后缀, 可以不设置
        contentValues.put(MediaStore.Downloads.TITLE, "hello");

        // uri 表示操作哪个数据库 , contentValues 表示要插入的数据内容 下面2种操作都可以
//        Uri insert = getContentResolver().insert(uri, contentValues);
        Uri insert = getContentResolver().insert(MediaStore.Downloads.EXTERNAL_CONTENT_URI, contentValues);
        
        BufferedOutputStream bos = null;
        try {
            // 向 Download/hello/hello.txt 文件中插入数据
            OutputStream os = getContentResolver().openOutputStream(insert);
            bos = new BufferedOutputStream(os);
            bos.write("Hello World".getBytes("UTF8"));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(bos != null)
                    bos.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * 保存图标到相册中
     * @param v
     */
    public void onSave1(View v) {

    }


    /**
     * 
     * @param v
     */
    public void onSave2(View v) {
    }



}